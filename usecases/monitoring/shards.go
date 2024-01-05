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

package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Move the shard from unloaded to in progress
func (pm *PrometheusMetrics) StartLoadingShard(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}
	suld, err := pm.ShardsUnloaded.GetMetricWith(labels)
	if err != nil {
		return err
	}
	suld.Dec()

	slding, err := pm.ShardsLoading.GetMetricWith(labels)
	if err != nil {
		return err
	}

	slding.Inc()
	return nil
}

// Move the shard from in progress to loaded
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

// Move the shard from loaded to in progress
func (pm *PrometheusMetrics) StartUnloadingShard(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}

	sldd, err := pm.ShardsLoaded.GetMetricWith(labels)
	if err != nil {
		return err
	}

	sldd.Dec()

	suld, err := pm.ShardsUnloaded.GetMetricWith(labels)
	if err != nil {
		return err
	}

	suld.Inc()
	return nil
}

// Move the shard from in progress to unloaded
func (pm *PrometheusMetrics) FinishUnloadingShard(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}

	sulding, err := pm.ShardsUnloading.GetMetricWith(labels)
	if err != nil {
		return err
	}

	sulding.Dec()

	suld, err := pm.ShardsUnloaded.GetMetricWith(labels)
	if err != nil {
		return err
	}

	suld.Inc()

	return nil
}

// Register a new, unloaded shard
func (pm *PrometheusMetrics) NewUnloadedshard(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}

	suld, err := pm.ShardsUnloaded.GetMetricWith(labels)
	if err != nil {
		return err
	}

	suld.Inc()
	return nil
}
