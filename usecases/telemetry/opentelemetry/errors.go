//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package opentelemetry

import "errors"

var (
	// ErrEmptyServiceName is returned when the service name is empty
	ErrEmptyServiceName = errors.New("opentelemetry: service name cannot be empty")

	// ErrEmptyExporterEndpoint is returned when the exporter endpoint is empty
	ErrEmptyExporterEndpoint = errors.New("opentelemetry: exporter endpoint cannot be empty")

	// ErrInvalidSamplingRate is returned when the sampling rate is invalid
	ErrInvalidSamplingRate = errors.New("opentelemetry: sampling rate must be between 0.0 and 1.0")

	// ErrInvalidBatchTimeout is returned when the batch timeout is invalid
	ErrInvalidBatchTimeout = errors.New("opentelemetry: batch timeout must be greater than 0")

	// ErrInvalidBatchSize is returned when the batch size is invalid
	ErrInvalidBatchSize = errors.New("opentelemetry: batch size must be greater than 0")

	// ErrExporterNotSupported is returned when the exporter protocol is not supported
	ErrExporterNotSupported = errors.New("opentelemetry: exporter protocol not supported")
)
