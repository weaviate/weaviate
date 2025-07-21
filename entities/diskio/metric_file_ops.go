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
