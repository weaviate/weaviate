package fsm

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"google.golang.org/protobuf/proto"
)

type Response struct {
	Error   error
	Version uint64
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	ret := Response{Version: l.Index}
	if l.Type != raft.LogCommand {
		f.cfg.Logger.WithFields(logrus.Fields{
			"type":  l.Type,
			"index": l.Index,
		}).Warn("not a valid command")
		return ret
	}
	cmd := api.ApplyRequest{}
	if err := proto.Unmarshal(l.Data, &cmd); err != nil {
		f.cfg.Logger.WithError(err).Error("decode command")
		panic("error proto un-marshalling log data")
	}

	// schemaOnly is necessary so that on restart when we are re-applying RAFT log entries to our in-memory schema we
	// don't update the database. This can lead to data loss for example if we drop then re-add a class.
	// If we don't have any last applied index on start, schema only is always false.
	// we check for index !=0 to force apply of the 1st index in both db and schema
	catchingUp := l.Index != 0 && l.Index <= f.lastAppliedIndexToDB.Load()
	schemaOnly := catchingUp || f.cfg.MetadataOnly
	defer func() {
		// If we have an applied index from the previous store (i.e from disk). Then reload the DB once we catch up as
		// that means we're done doing schema only.
		if l.Index != 0 && l.Index == f.lastAppliedIndexToDB.Load() {
			f.cfg.Logger.WithFields(logrus.Fields{
				"log_type":                     l.Type,
				"log_name":                     l.Type.String(),
				"log_index":                    l.Index,
				"last_store_log_applied_index": f.lastAppliedIndexToDB.Load(),
			}).Info("reloading local DB as RAFT and local DB are now caught up")
			f.reloadDBFromSchema()
		}

		f.lastAppliedIndex.Store(l.Index)

		if ret.Error != nil {
			f.cfg.Logger.WithFields(logrus.Fields{
				"log_type":      l.Type,
				"log_name":      l.Type.String(),
				"log_index":     l.Index,
				"cmd_type":      cmd.Type,
				"cmd_type_name": cmd.Type.String(),
				"cmd_class":     cmd.Class,
			}).WithError(ret.Error).Error("apply command")
		}
	}()

	cmd.Version = l.Index
	// Report only when not ready the progress made on applying log entries. This help users with big schema and long
	// startup time to keep track of progress.
	// By checking lastAppliedIndexToDB we ensure that we never print past that index
	if l.Index <= f.lastAppliedIndexToDB.Load() {
		f.cfg.Logger.Infof("Schema catching up: applying log entry: [%d/%d]", l.Index, f.lastAppliedIndexToDB.Load())
	}
	f.cfg.Logger.WithFields(logrus.Fields{
		"log_type":        l.Type,
		"log_name":        l.Type.String(),
		"log_index":       l.Index,
		"cmd_type":        cmd.Type,
		"cmd_type_name":   cmd.Type.String(),
		"cmd_class":       cmd.Class,
		"cmd_schema_only": schemaOnly,
	}).Debug("server.apply")

	applyOp := func() {}

	switch cmd.Type {

	case api.ApplyRequest_TYPE_ADD_CLASS:
		applyOp = func() {
			ret.Error = f.schemaManager.AddClass(&cmd, f.cfg.NodeID, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_RESTORE_CLASS:
		applyOp = func() {
			ret.Error = f.schemaManager.RestoreClass(&cmd, f.cfg.NodeID, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_UPDATE_CLASS:
		applyOp = func() {
			ret.Error = f.schemaManager.UpdateClass(&cmd, f.cfg.NodeID, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_DELETE_CLASS:
		applyOp = func() {
			ret.Error = f.schemaManager.DeleteClass(&cmd, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_ADD_PROPERTY:
		applyOp = func() {
			ret.Error = f.schemaManager.AddProperty(&cmd, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:
		applyOp = func() {
			ret.Error = f.schemaManager.UpdateShardStatus(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_ADD_TENANT:
		applyOp = func() {
			ret.Error = f.schemaManager.AddTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_UPDATE_TENANT:
		applyOp = func() {
			ret.Error = f.schemaManager.UpdateTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_DELETE_TENANT:
		applyOp = func() {
			ret.Error = f.schemaManager.DeleteTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_TENANT_PROCESS:
		applyOp = func() {
			ret.Error = f.schemaManager.UpdateTenantsProcess(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_STORE_SCHEMA_V1:
		applyOp = func() {
			// ret.Error = f.StoreSchemaV1()
		}

	case api.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS:
		applyOp = func() {
			ret.Error = f.rbacManager.UpsertRolesPermissions(&cmd)
		}
	case api.ApplyRequest_TYPE_DELETE_ROLES:
		applyOp = func() {
			ret.Error = f.rbacManager.DeleteRoles(&cmd)
		}
	case api.ApplyRequest_TYPE_REMOVE_PERMISSIONS:
		applyOp = func() {
			ret.Error = f.rbacManager.RemovePermissions(&cmd)
		}
	case api.ApplyRequest_TYPE_ADD_ROLES_FOR_USER:
		applyOp = func() {
			ret.Error = f.rbacManager.AddRolesForUser(&cmd)
		}
	case api.ApplyRequest_TYPE_REVOKE_ROLES_FOR_USER:
		applyOp = func() {
			ret.Error = f.rbacManager.RevokeRolesForUser(&cmd)
		}

	case api.ApplyRequest_TYPE_UPSERT_USER:
		applyOp = func() {
			ret.Error = f.dynUserManager.CreateUser(&cmd)
		}
	case api.ApplyRequest_TYPE_DELETE_USER:
		applyOp = func() {
			ret.Error = f.dynUserManager.DeleteUser(&cmd)
		}
	case api.ApplyRequest_TYPE_ROTATE_USER_API_KEY:
		applyOp = func() {
			ret.Error = f.dynUserManager.RotateKey(&cmd)
		}
	case api.ApplyRequest_TYPE_SUSPEND_USER:
		applyOp = func() {
			ret.Error = f.dynUserManager.SuspendUser(&cmd)
		}
	case api.ApplyRequest_TYPE_ACTIVATE_USER:
		applyOp = func() {
			ret.Error = f.dynUserManager.ActivateUser(&cmd)
		}
	default:
		// This could occur when a new command has been introduced in a later app version
		// At this point, we need to panic so that the app undergo an upgrade during restart
		const msg = "consider upgrading to newer version"
		f.cfg.Logger.WithFields(logrus.Fields{
			"type":  cmd.Type,
			"class": cmd.Class,
			"more":  msg,
		}).Error("unknown command")
		panic(fmt.Sprintf("unknown command type=%d class=%s more=%s", cmd.Type, cmd.Class, msg))
	}

	// Wrap the function in a go routine to ensure panic recovery. This is necessary as this function is run in an
	// unwrapped goroutine in the raft library
	wg := sync.WaitGroup{}
	wg.Add(1)
	g := func() {
		applyOp()
		wg.Done()
	}
	enterrors.GoWrapper(g, f.cfg.Logger)
	wg.Wait()

	return ret

}
