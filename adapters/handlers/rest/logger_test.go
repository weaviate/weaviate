//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/banner"
)

const multiLineMsg = "\n line one\n line two\n"

func TestWeaviateTextFormatter(t *testing.T) {
	tests := []struct {
		name   string
		fields logrus.Fields
		msg    string
		// wantRaw is the tail expected verbatim after the header line; empty
		// means the message is expected quoted inside the header instead.
		wantRaw string
	}{
		{
			name:   "banner entry keeps the header and appends the message verbatim",
			fields: logrus.Fields{"action": banner.Action},
			msg:    multiLineMsg,
			// The header line is terminated by one newline, then the message
			// follows verbatim.
			wantRaw: "\n" + multiLineMsg,
		},
		{
			name:   "banner message without a trailing newline gets one",
			fields: logrus.Fields{"action": banner.Action},
			msg:    "single line, no newline",
			// Without the added newline the next log entry would be glued
			// onto this line.
			wantRaw: "\nsingle line, no newline\n",
		},
		{
			name:   "other multi-line entries stay quoted on one line",
			fields: logrus.Fields{"action": "startup"},
			msg:    multiLineMsg,
		},
		{
			name:   "entry without action stays quoted on one line",
			fields: logrus.Fields{},
			msg:    multiLineMsg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := logrus.New()
			logger.SetOutput(&buf)
			logger.SetFormatter(&WeaviateTextFormatter{
				TextFormatter: &logrus.TextFormatter{DisableColors: true, DisableTimestamp: true},
				gitHash:       "abc123",
			})

			logger.WithFields(tt.fields).Info(tt.msg)
			out := buf.String()

			assert.Contains(t, out, "build_git_commit=abc123", "build fields are still attached")

			if tt.wantRaw == "" {
				lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
				assert.Len(t, lines, 1, "message with newlines is %%q-quoted onto one line")
				assert.Contains(t, out, `msg="\n line one\n line two\n"`)
				return
			}

			assert.True(t, strings.HasSuffix(out, tt.wantRaw), "output %q does not end with the raw message", out)
			assert.NotContains(t, out, `msg=`, "the banner message is not emitted as a quoted field")
			header := strings.TrimSuffix(out, tt.wantRaw)
			assert.NotContains(t, header, "\n", "everything before the message is a single header line")
		})
	}
}

func TestWeaviateJSONFormatterKeepsMultiLineMessageOnOneLine(t *testing.T) {
	var buf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&buf)
	logger.SetFormatter(&WeaviateJSONFormatter{JSONFormatter: &logrus.JSONFormatter{}, gitHash: "abc123"})

	logger.WithField("action", banner.Action).Info(multiLineMsg)

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 1)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &entry))
	assert.Equal(t, multiLineMsg, entry["msg"], "newlines round-trip through JSON escaping")
	assert.Equal(t, "abc123", entry["build_git_commit"])
}
