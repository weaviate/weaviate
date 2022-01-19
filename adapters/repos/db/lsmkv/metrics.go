package lsmkv

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

type Metrics struct {
	CompactionReplace *prometheus.GaugeVec
	CompactionSet     *prometheus.GaugeVec
	CompactionMap     *prometheus.GaugeVec
	ActiveSegments    *prometheus.GaugeVec
}

func NewMetrics(promMetrics *monitoring.PrometheusMetrics, className,
	shardName string) *Metrics {
	replace := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratreplace",
		"class_name": className,
		"shard_name": shardName,
	})

	set := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratset",
		"class_name": className,
		"shard_name": shardName,
	})

	stratMap := promMetrics.AsyncOperations.MustCurryWith(prometheus.Labels{
		"operation":  "compact_lsm_segments_stratmap",
		"class_name": className,
		"shard_name": shardName,
	})

	return &Metrics{
		CompactionReplace: replace,
		CompactionSet:     set,
		CompactionMap:     stratMap,
		ActiveSegments: promMetrics.LSMSegmentCount.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
	}
}
