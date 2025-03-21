package fsm

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/fsm/dynusers"
	"github.com/weaviate/weaviate/cluster/fsm/rbac"
	"github.com/weaviate/weaviate/cluster/fsm/schema"
	"github.com/weaviate/weaviate/cluster/types"
)

type FSM struct {
	cfg FSMConfig

	// Sub-FSM specialized
	schemaManager  *schema.SchemaManager
	rbacManager    *rbac.Manager
	dynUserManager *dynusers.Manager

	// lastAppliedIndex index of latest update to the store
	lastAppliedIndex atomic.Uint64
	// lastAppliedIndexToDB represents the index of the last applied command when the store is opened.
	// It *must* be inited from the max found in log store & snapshot store in order to ensure the FSM will not apply any update to the DB
	// it is not meant to apply to it
	lastAppliedIndexToDB atomic.Uint64
	dbLoaded             atomic.Bool
}

// Ensure that FSM implements the necessary raft interface
var _ raft.FSM = &FSM{}

func NewFSM(cfg FSMConfig, schemaManager *schema.SchemaManager, rbacManager *rbac.Manager) *FSM {
	f := &FSM{
		cfg:           cfg,
		schemaManager: schemaManager,
		rbacManager:   rbacManager,
	}
	return f
}

func (f *FSM) Open(ctx context.Context, lastIndex uint64) {
	if f.dbLoaded.Load() {
		return
	}

	f.lastAppliedIndexToDB.Store(lastIndex)

	if f.cfg.MetadataOnly {
		f.cfg.Logger.Info("Not loading local DB as the node is metadata only")
	} else {
		f.cfg.Logger.Info("loading local db")
		if err := f.schemaManager.Load(ctx, f.cfg.NodeID); err != nil {
			f.cfg.Logger.WithError(err).Error("cannot restore database")
			panic("error restoring database")
		}
		f.cfg.Logger.Info("local DB successfully loaded")
		f.dbLoaded.Store(true)
	}

	f.cfg.Logger.WithField("n", f.schemaManager.NewSchemaReader().Len()).Info("schema manager loaded")
}

func (f *FSM) Close(ctx context.Context) error {
	return f.schemaManager.Close(ctx)
}

func (f *FSM) NewSchemaReader() schema.SchemaReader {
	waitFunc := func(ctx context.Context, version uint64) error {
		return f.waitForAppliedIndex(ctx, time.Millisecond*50, version)
	}
	return f.schemaManager.NewSchemaReaderWithWaitFunc(waitFunc)
}

func (f *FSM) SetDBHandle(dbHandle schema.Indexer) {
	f.schemaManager.SetIndexer(dbHandle)
}

func (f *FSM) IsDBOpen() bool {
	return f.dbLoaded.Load()
}

func (f *FSM) reloadDBFromSchema() {
	if !f.cfg.MetadataOnly {
		f.schemaManager.ReloadDBFromSchema()
	} else {
		f.cfg.Logger.Info("skipping reload DB from schema as the node is metadata only")
	}
	f.dbLoaded.Store(true)
}

func (f *FSM) waitForAppliedIndex(ctx context.Context, period time.Duration, version uint64) error {
	if idx := f.lastAppliedIndex.Load(); idx >= version {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, f.cfg.ConsistencyWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	var idx uint64
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: version got=%d  want=%d", types.ErrDeadlineExceeded, idx, version)
		case <-ticker.C:
			if idx = f.lastAppliedIndex.Load(); idx >= version {
				return nil
			} else {
				f.cfg.Logger.WithFields(logrus.Fields{
					"got":  idx,
					"want": version,
				}).Debug("wait for update version")
			}
		}
	}
}
