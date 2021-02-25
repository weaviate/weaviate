//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package moduletools

type ModuleInitParams interface {
	GetStorageProvider() StorageProvider
	GetAppState() interface{}
}

type InitParams struct {
	storageProvider StorageProvider
	appState        interface{}
}

func NewInitParams(storageProvider StorageProvider, appState interface{}) ModuleInitParams {
	return &InitParams{storageProvider, appState}
}

func (p *InitParams) GetStorageProvider() StorageProvider {
	return p.storageProvider
}

func (p *InitParams) GetAppState() interface{} {
	return p.appState
}
