//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

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
	BloomFilters      prometheus.ObserverVec
	SegmentObjects    *prometheus.GaugeVec
	SegmentSize       *prometheus.GaugeVec
	SegmentCount      *prometheus.GaugeVec
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
		BloomFilters: promMetrics.LSMBloomFilters.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentObjects: promMetrics.LSMSegmentObjects.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentSize: promMetrics.LSMSegmentSize.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentCount: promMetrics.LSMSegmentCountByLevel.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
	}
}
