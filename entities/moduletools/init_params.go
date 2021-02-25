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
