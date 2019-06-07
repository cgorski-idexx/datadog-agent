// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

package docker

import (
	"encoding/json"
	"fmt"

	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/DataDog/datadog-agent/pkg/logs/parser"
)

// stream types.
const (
	stderr = "stderr"
	stdout = "stdout"
)

// JSONParser is a shared json parser.
var JSONParser parser.Parser = &jsonParser{}

// logLine contains all the attributes of a container log.
type logLine struct {
	Log    string
	Stream string
	Time   string
}

// jsonParser parses raw JSON lines to log fields.
type jsonParser struct{}

// Parse parses a raw JSON line to a container line and then returns all its fields,
// returns an error if it failed.
// For example:
// {"log":"a message","stream":"stderr","time":"2019-06-06T16:35:55.930852911Z"}
// returns:
// "a message", "error", "2019-06-06T16:35:55.930852911Z", nil
func (p *jsonParser) Parse(data []byte) ([]byte, string, string, error) {
	var log *logLine
	err := json.Unmarshal(data, &log)
	if err != nil {
		return data, message.StatusInfo, "", fmt.Errorf("could not parse JSON container line: %v", err)
	}
	var status string
	switch log.Stream {
	case stderr:
		status = message.StatusError
	case stdout:
		status = message.StatusInfo
	default:
		status = ""
	}
	return []byte(log.Log), status, log.Time, nil
}
