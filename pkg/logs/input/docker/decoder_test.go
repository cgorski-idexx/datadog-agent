// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

// +build docker

package docker

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/datadog-agent/pkg/logs/config"
	"github.com/DataDog/datadog-agent/pkg/logs/decoder"
	"github.com/DataDog/datadog-agent/pkg/logs/message"
)

func TestDecoderWithSingleline(t *testing.T) {
	var output *decoder.Output

	d := decoder.InitializeDecoder(config.NewLogSource("", &config.LogsConfig{}), JSONParser)
	d.Start()
	defer d.Stop()

	d.InputChan <- decoder.NewInput([]byte(`{"log":"message","stream":"stdout","time":"2019-06-06T16:35:55.930852911Z" }` + "\n"))
	output = <-d.OutputChan
	assert.Equal(t, []byte("message"), output.Content)
	assert.Equal(t, message.StatusInfo, output.Status)
	assert.Equal(t, "2019-06-06T16:35:55.930852911Z", output.Timestamp)

	d.InputChan <- decoder.NewInput([]byte(`wrong message` + "\n"))
	output = <-d.OutputChan
	assert.Equal(t, []byte("wrong message"), output.Content)
	assert.Equal(t, message.StatusInfo, output.Status)
	assert.Equal(t, "", output.Timestamp)
}

func TestDecoderWithMultiline(t *testing.T) {
	var output *decoder.Output

	c := &config.LogsConfig{
		ProcessingRules: []*config.ProcessingRule{
			{
				Type:  config.MultiLine,
				Regex: regexp.MustCompile("1234"),
			},
		},
	}
	d := decoder.InitializeDecoder(config.NewLogSource("", c), JSONParser)
	d.Start()
	defer d.Stop()

	d.InputChan <- decoder.NewInput([]byte(`{"log":"1234 hello","stream":"stdout","time":"2019-06-06T16:35:55.930852911Z" }` + "\n"))
	d.InputChan <- decoder.NewInput([]byte(`{"log":"world","stream":"stdout","time":"2019-06-06T16:35:55.930852912Z" }` + "\n"))
	d.InputChan <- decoder.NewInput([]byte(`{"log":"1234 bye","stream":"stdout","time":"2019-06-06T16:35:55.930852913Z" }` + "\n"))
	output = <-d.OutputChan
	assert.Equal(t, []byte("1234 hello\\nworld"), output.Content)
	assert.Equal(t, message.StatusInfo, output.Status)
	assert.Equal(t, "2019-06-06T16:35:55.930852912Z", output.Timestamp)

	output = <-d.OutputChan
	assert.Equal(t, []byte("1234 bye"), output.Content)
	assert.Equal(t, message.StatusInfo, output.Status)
	assert.Equal(t, "2019-06-06T16:35:55.930852913Z", output.Timestamp)
}
