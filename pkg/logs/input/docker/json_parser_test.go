// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

package docker

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/datadog-agent/pkg/logs/message"
)

func TestJSONParser(t *testing.T) {
	var (
		content   []byte
		status    string
		timestamp string
		err       error
	)

	parser := JSONParser

	content, status, timestamp, err = parser.Parse([]byte(`{"log":"a message","stream":"stderr","time":"2019-06-06T16:35:55.930852911Z"}`))
	assert.Nil(t, err)
	assert.Equal(t, []byte("a message"), content)
	assert.Equal(t, message.StatusError, status)
	assert.Equal(t, "2019-06-06T16:35:55.930852911Z", timestamp)

	content, status, timestamp, err = parser.Parse([]byte(`{"log":"a second message","stream":"stdout","time":"2019-06-06T16:35:55.930852911Z"}`))
	assert.Nil(t, err)
	assert.Equal(t, []byte("a second message"), content)
	assert.Equal(t, message.StatusInfo, status)
	assert.Equal(t, "2019-06-06T16:35:55.930852911Z", timestamp)

	content, status, _, err = parser.Parse([]byte("a wrong message"))
	assert.NotNil(t, err)
	assert.Equal(t, []byte("a wrong message"), content)
	assert.Equal(t, message.StatusInfo, status)
}
