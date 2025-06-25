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

package moduletools

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/config"
)

type ModuleInitParams interface {
	GetStorageProvider() StorageProvider
	GetAppState() interface{}
	GetLogger() logrus.FieldLogger
	GetConfig() *config.Config
	GetMetricsRegisterer() prometheus.Registerer
}

type InitParams struct {
	storageProvider StorageProvider
	appState        interface{}
	config          *config.Config
	logger          logrus.FieldLogger
	registerer      prometheus.Registerer
}

func NewInitParams(storageProvider StorageProvider, appState interface{},
	config *config.Config, logger logrus.FieldLogger, registerer prometheus.Registerer,
) ModuleInitParams {
	return &InitParams{storageProvider, appState, config, logger, registerer}
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

func (p *InitParams) GetConfig() *config.Config {
	return p.config
}

func (p *InitParams) GetMetricsRegisterer() prometheus.Registerer {
	return p.registerer
}
