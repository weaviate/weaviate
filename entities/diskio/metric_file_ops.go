//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package diskio

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func CreateFile(path, source string) (*os.File, error) {
	metric := monitoring.GetMetrics().FileIOOps
	if metric != nil {
		metric.With(prometheus.Labels{
			"operation": "create_file",
			"source":    source,
		}).Inc()
	}
	return os.Create(path)
}

func Rename(old, new, source string) error {
	metric := monitoring.GetMetrics().FileIOOps
	if metric != nil {
		metric.With(prometheus.Labels{
			"operation": "rename_file",
			"source":    source,
		}).Inc()
	}
	return os.Rename(old, new)
}

func Remove(path, source string) error {
	metric := monitoring.GetMetrics().FileIOOps
	if metric != nil {
		metric.With(prometheus.Labels{
			"operation": "remove_file",
			"source":    source,
		}).Inc()
	}

	return os.Remove(path)
}

func RemoveAll(path, source string) error {
	metric := monitoring.GetMetrics().FileIOOps
	if metric != nil {
		metric.With(prometheus.Labels{
			"operation": "remove_all_file",
			"source":    source,
		}).Inc()
	}

	return os.RemoveAll(path)
}
