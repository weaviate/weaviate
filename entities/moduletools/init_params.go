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

package moduletools

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type ModuleInitParams interface {
	GetStorageProvider() StorageProvider
	GetAppState() interface{}
	GetLogger() logrus.FieldLogger
	GetMetrics() Metrics
}

type InitParams struct {
	storageProvider StorageProvider
	appState        interface{}
	logger          logrus.FieldLogger
	metrics         *moduleMetrics
}

func NewInitParams(storageProvider StorageProvider, appState interface{},
	logger logrus.FieldLogger,
	metrics *monitoring.PrometheusMetrics,
) ModuleInitParams {
	return &InitParams{storageProvider, appState, logger, newModuleMetrics(metrics)}
}

func (p *InitParams) GetStorageProvider() StorageProvider {
	return p.storageProvider
}

func (p *InitParams) GetAppState() interface{} {
	return p.appState
}

func (p *InitParams) GetLogger() logrus.FieldLogger {
	return p.logger
}

func (p *InitParams) GetMetrics() Metrics {
	return p.metrics
}
