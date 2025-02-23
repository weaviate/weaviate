package runtimeconfig

import (
	"time"
)

type ConfigGetter interface {
	GetConfig() (*Weaviate, error)
}

type Weaviate struct {
	cm ConfigGetter

	BackupInterval time.Duration `yaml:"backup_interval"`
}

func (w *Weaviate) BackupInternal() time.Duration {
	w.cm
}
