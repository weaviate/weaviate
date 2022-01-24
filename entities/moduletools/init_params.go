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

package moduletools

import "github.com/sirupsen/logrus"

type ModuleInitParams interface {
	GetStorageProvider() StorageProvider
	GetAppState() interface{}
	GetLogger() logrus.FieldLogger
}

type InitParams struct {
	storageProvider StorageProvider
	appState        interface{}
	logger          logrus.FieldLogger
}

func NewInitParams(storageProvider StorageProvider, appState interface{},
	logger logrus.FieldLogger) ModuleInitParams {
	return &InitParams{storageProvider, appState, logger}
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
