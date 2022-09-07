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

package backup

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/sirupsen/logrus"
)

type restorer struct {
	logger     logrus.FieldLogger
	sourcer    Sourcer
	backends   BackupBackendProvider
	schema     schemaManger
	lastStatus backupStat
}

func newRestorer(logger logrus.FieldLogger,
	sourcer Sourcer,
	backends BackupBackendProvider,
	schema schemaManger,
) *restorer {
	return &restorer{
		logger:   logger,
		sourcer:  sourcer,
		backends: backends,
		schema:   schema,
	}
}

func (r *restorer) restoreAll(ctx context.Context,
	pr *models.Principal,
	desc *backup.BackupDescriptor,
	store objectStore,
) (err error) {
	backupID := desc.ID
	// make sure there is no active restore
	dst := store.HomeDir(backupID)
	if prevID := r.lastStatus.renew(backupID, time.Now(), dst); prevID != "" {
		err := fmt.Errorf("restore %s already in progress", prevID)
		return err
	}
	defer func() {
		r.lastStatus.reset()
	}()

	r.lastStatus.set(backup.Transferring)
	for _, cdesc := range desc.Classes {
		if err := r.restoreOne(ctx, pr, backupID, &cdesc, store); err != nil {
			return fmt.Errorf("restore class %s: %w", cdesc.Name, err)
		}
		r.logger.WithField("action", "restore").
			WithField("backup_id", desc.ID).
			WithField("class", cdesc.Name)
	}
	return nil
}

func getType(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func (r *restorer) restoreOne(ctx context.Context,
	pr *models.Principal, backupID string,
	desc *backup.ClassDescriptor,
	store objectStore,
) (err error) {
	timer := prometheus.NewTimer(monitoring.GetMetrics().BackupRestoreDurations.WithLabelValues(getType(store.BackupBackend), desc.Name))
	defer timer.ObserveDuration()

	if r.sourcer.ClassExists(desc.Name) {
		return fmt.Errorf("already exists")
	}
	fw := newFileWriter(r.sourcer, store, backupID)
	rollback, err := fw.Write(ctx, desc)
	if err != nil {
		return fmt.Errorf("write files: %w", err)
	}
	if err := r.schema.RestoreClass(ctx, pr, desc); err != nil {
		if rerr := rollback(); rerr != nil {
			r.logger.WithField("className", desc.Name).WithField("action", "rollback").WithError(rerr)
		}
		return fmt.Errorf("restore schema: %w", err)
	}
	return nil
}

// AnyExists check if any class of cs exists in DB
func (r *restorer) AnyExists(cs []string) string {
	for _, cls := range cs {
		if r.sourcer.ClassExists(cls) {
			return cls
		}
	}
	return ""
}

func (r *restorer) status() reqStat {
	return r.lastStatus.get()
}
