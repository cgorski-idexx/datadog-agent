// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

package decoder

import (
	"bytes"
	"github.com/DataDog/datadog-agent/pkg/logs/config"
	"github.com/DataDog/datadog-agent/pkg/logs/parser"
)

// defaultContentLenLimit represents the length limit above which we want to
// truncate the output content
const defaultContentLenLimit = 256 * 1000

// Input represents a list of bytes consumed by the Decoder
type Input struct {
	content []byte
}

// NewInput returns a new input
func NewInput(content []byte) *Input {
	return &Input{content}
}

// Output represents the fields parsed from decoder.
type Output struct {
	Content    []byte
	Status     string
	RawDataLen int
	Timestamp  string
}

// NewOutput returns a new output.
func NewOutput(content []byte, status string, rawDataLen int, timestamp string) *Output {
	return &Output{
		Content:    content,
		Status:     status,
		RawDataLen: rawDataLen,
		Timestamp:  timestamp,
	}
}

// Decoder splits raw data into lines and passes them to a lineHandler that emits outputs
type Decoder struct {
	InputChan  chan *Input
	OutputChan chan *Output
	EndLineMatcher
	lineBuffer      *bytes.Buffer
	lineHandler     LineHandler
	contentLenLimit int
}

// InitializeDecoder returns a properly initialized Decoder
func InitializeDecoder(source *config.LogSource, parser parser.Parser) *Decoder {
	return NewDecoderWithEndLineMatcher(source, parser, &newLineMatcher{})
}

// NewDecoderWithEndLineMatcher initialize a decoder with given endline strategy.
func NewDecoderWithEndLineMatcher(source *config.LogSource, parser parser.Parser, matcher EndLineMatcher) *Decoder {
	inputChan := make(chan *Input)
	outputChan := make(chan *Output)
	lineLimit := defaultContentLenLimit
	var lineHandler LineHandler
	for _, rule := range source.Config.ProcessingRules {
		if rule.Type == config.MultiLine {
			lineHandler = NewMultiLineHandler(outputChan, rule.Regex, defaultFlushTimeout, parser, lineLimit)
		}
	}
	if lineHandler == nil {
		lineHandler = NewSingleLineHandler(outputChan, parser, lineLimit)
	}

	return New(inputChan, outputChan, lineHandler, lineLimit, matcher)
}

// New returns an initialized Decoder
func New(InputChan chan *Input, OutputChan chan *Output, lineHandler LineHandler, contentLenLimit int, matcher EndLineMatcher) *Decoder {
	var lineBuffer bytes.Buffer
	return &Decoder{
		InputChan:       InputChan,
		OutputChan:      OutputChan,
		lineBuffer:      &lineBuffer,
		lineHandler:     lineHandler,
		contentLenLimit: contentLenLimit,
		EndLineMatcher:  matcher,
	}
}

// Start starts the Decoder
func (d *Decoder) Start() {
	d.lineHandler.Start()
	go d.run()
}

// Stop stops the Decoder
func (d *Decoder) Stop() {
	close(d.InputChan)
}

// run lets the Decoder handle data coming from InputChan
func (d *Decoder) run() {
	for data := range d.InputChan {
		d.decodeIncomingData(data.content)
	}
	// finish to stop decoder
	d.lineHandler.Stop()
}

// decodeIncomingData splits raw data based on '\n', creates and processes new lines
func (d *Decoder) decodeIncomingData(inBuf []byte) {
	i, j := 0, 0
	n := len(inBuf)
	maxj := d.contentLenLimit - d.lineBuffer.Len()

	for ; j < n; j++ {
		if j == maxj {
			// send line because it is too long
			d.lineBuffer.Write(inBuf[i:j])
			d.sendLine()
			i = j
			maxj = i + d.contentLenLimit
		} else if d.Match(d.lineBuffer.Bytes(), inBuf, i, j) {
			d.lineBuffer.Write(inBuf[i:j])
			d.sendLine()
			i = j + 1 // skip the matching byte.
			maxj = i + d.contentLenLimit
		}
	}
	d.lineBuffer.Write(inBuf[i:j])
}

// sendLine copies content from lineBuffer which is passed to lineHandler
func (d *Decoder) sendLine() {
	content := make([]byte, d.lineBuffer.Len())
	copy(content, d.lineBuffer.Bytes())
	d.lineBuffer.Reset()
	d.lineHandler.Handle(content)
}
