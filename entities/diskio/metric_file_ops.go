package diskio

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func CreateFile(path, source string) (*os.File, error) {
	monitoring.GetMetrics().FileIOOps.With(prometheus.Labels{
		"operation": "create_file",
		"source":    source,
	})
	return os.Create(path)
}
