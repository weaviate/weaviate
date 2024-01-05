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

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	tlog "github.com/sirupsen/logrus/hooks/test"
)

func TestBackupMutex(t *testing.T) {
	l, _ := tlog.NewNullLogger()
	t.Run("success first time", func(t *testing.T) {
		m := backupMutex{log: l, retryDuration: time.Millisecond, notifyDuration: 5 * time.Millisecond}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Millisecond)
		defer cancel()
		if err := m.LockWithContext(ctx); err != nil {
			t.Errorf("error want:nil got:%v ", err)
		}
	})
	t.Run("success after retry", func(t *testing.T) {
		m := backupMutex{log: l, retryDuration: 2 * time.Millisecond, notifyDuration: 5 * time.Millisecond}
		m.RLock()
		go func() {
			defer m.RUnlock()
			time.Sleep(time.Millisecond * 15)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.LockWithContext(ctx); err != nil {
			t.Errorf("error want:nil got:%v ", err)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		m := backupMutex{log: l, retryDuration: time.Millisecond, notifyDuration: 5 * time.Millisecond}
		m.RLock()
		defer m.RUnlock()
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Millisecond)
		defer cancel()
		err := m.LockWithContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error want:%v got:%v", err, context.DeadlineExceeded)
		}
	})
}
