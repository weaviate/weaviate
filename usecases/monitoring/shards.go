//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Delete Shard deletes existing label combinations that match both
// the shard and class name. If a metric is not collected at the shard
// level it is unaffected. This is to make sure that deleting a single
// shard (e.g. multi-tenancy) does not affect metrics for existing
// shards.
//
// In addition, there are some metrics that we explicitly keep, such
// as vector_dimensions_sum as they can be used in billing decisions.
func (pm *PrometheusMetrics) StartLoadingShard(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}
	suld, err := pm.ShardsUnLoaded.GetMetricWith(labels)
	if err != nil {
		return err
	}
	suld.Dec()

	slding,err := pm.ShardsLoading.GetMetricWith(labels)
	if err != nil {
		return err
	}

	slding.Inc()
	return nil
}

func (pm *PrometheusMetrics) FinishLoadingShard(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}

	slding, err := pm.ShardsLoading.GetMetricWith(labels)
	if err != nil {
		return err
	}

	slding.Dec()

	sldd, err := pm.ShardsLoaded.GetMetricWith(labels)
	if err != nil {
		return err
	}

	sldd.Inc()
	return nil
}

func (pm *PrometheusMetrics) NewUnLoadedshard(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}

	suld, err := pm.ShardsUnLoaded.GetMetricWith(labels)
	if err != nil {
		return err
	}

	suld.Inc()
	return nil
}