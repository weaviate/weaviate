//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Config for a new HSNW index
type Config struct {
	RootPath              string
	ID                    string
	MakeCommitLoggerThunk MakeCommitLogger
	MaximumConnections    int
	EFConstruction        int
	VectorForIDThunk      VectorForID

	// Optional, no period clean up will be scheduled if interval is not set
	TombstoneCleanupInterval time.Duration
}

func (c Config) Validate() error {
	ec := &errorCompounder{}

	if c.ID == "" {
		ec.addf("id cannot be empty")
	}

	if c.RootPath == "" {
		ec.addf("rootPath cannot be empty")
	}

	if c.MaximumConnections <= 0 {
		ec.addf("maximumConnections must be greater than 0")
	}

	if c.EFConstruction <= 0 {
		ec.addf("efConstruction must be greater than 0")
	}

	if c.MakeCommitLoggerThunk == nil {
		ec.addf("makeCommitLoggerThunk cannot be nil")
	}

	if c.VectorForIDThunk == nil {
		ec.addf("vectorForIDThunk cannot be nil")
	}

	return ec.toError()
}

type errorCompounder struct {
	errors []error
}

func (ec *errorCompounder) addf(msg string, args ...interface{}) {
	ec.errors = append(ec.errors, fmt.Errorf(msg, args...))
}

func (ec *errorCompounder) add(err error) {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
}

func (ec *errorCompounder) toError() error {
	if len(ec.errors) == 0 {
		return nil
	}

	var msg strings.Builder
	for i, err := range ec.errors {
		if i != 0 {
			msg.WriteString(", ")
		}

		msg.WriteString(err.Error())
	}

	return errors.New(msg.String())
}
