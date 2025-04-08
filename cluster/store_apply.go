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

package cluster

import (
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	gproto "google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func (st *Store) Execute(req *api.ApplyRequest) (uint64, error) {
	st.log.WithFields(logrus.Fields{
		"type":  api.ApplyRequest_Type_name[int32(req.Type)],
		"class": req.Class,
	}).Debug("server.execute")

	// Parse the underlying command before pre execute filtering to avoid queryinf the schema is the underlying command
	// is invalid
	cmdBytes, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal command: %w", err)
	}

	// Call the filtering to avoid committing to the FSM unnecessary updates
	if err := st.schemaManager.PreApplyFilter(req); err != nil {
		return 0, err
	}

	// The change is validated, we can apply it in RAFT
	fut := st.raft.Apply(cmdBytes, st.applyTimeout)

	// Always call Error first otherwise the response can't  be read from the future
	if err := fut.Error(); err != nil {
		return 0, err
	}

	// Always wait for the response
	futureResponse := fut.Response()
	resp, ok := futureResponse.(Response)
	if !ok {
		// This should not happen, but it's better to log an error *if* it happens than panic and crash.
		return 0, fmt.Errorf("response returned from raft apply is not of type Response instead got: %T, this should not happen!", futureResponse)
	}
	return resp.Version, resp.Error
}

// Apply is called once a log entry is committed by a majority of the cluster.
// Apply should apply the log to the FSM. Apply must be deterministic and
// produce the same result on all peers in the cluster.
// The returned value is returned to the client as the ApplyFuture.Response.
func (st *Store) Apply(l *raft.Log) interface{} {
	ret := Response{Version: l.Index}
	if l.Type != raft.LogCommand {
		st.log.WithFields(logrus.Fields{
			"type":  l.Type,
			"index": l.Index,
		}).Warn("not a valid command")
		return ret
	}
	cmd := api.ApplyRequest{}
	if err := gproto.Unmarshal(l.Data, &cmd); err != nil {
		st.log.WithError(err).Error("decode command")
		panic("error proto un-marshalling log data")
	}

	// schemaOnly is necessary so that on restart when we are re-applying RAFT log entries to our in-memory schema we
	// don't update the database. This can lead to data loss for example if we drop then re-add a class.
	// If we don't have any last applied index on start, schema only is always false.
	// we check for index !=0 to force apply of the 1st index in both db and schema
	catchingUp := l.Index != 0 && l.Index <= st.lastAppliedIndexToDB.Load()
	schemaOnly := catchingUp || st.cfg.MetadataOnlyVoters
	defer func() {
		// If we have an applied index from the previous store (i.e from disk). Then reload the DB once we catch up as
		// that means we're done doing schema only.
		if l.Index != 0 && l.Index == st.lastAppliedIndexToDB.Load() {
			st.log.WithFields(logrus.Fields{
				"log_type":                     l.Type,
				"log_name":                     l.Type.String(),
				"log_index":                    l.Index,
				"last_store_log_applied_index": st.lastAppliedIndexToDB.Load(),
			}).Info("reloading local DB as RAFT and local DB are now caught up")
			st.reloadDBFromSchema()
		}

		st.lastAppliedIndex.Store(l.Index)

		if ret.Error != nil {
			st.log.WithFields(logrus.Fields{
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
	// We check for ready state and index <= lastAppliedIndexToDB because just checking ready state would mean this log line
	// would keep printing if the node has caught up but there's no leader in the cluster.
	// This can happen for example if quorum is lost briefly.
	// By checking lastAppliedIndexToDB we ensure that we never print past that index
	if !st.Ready() && l.Index <= st.lastAppliedIndexToDB.Load() {
		st.log.Infof("Schema catching up: applying log entry: [%d/%d]", l.Index, st.lastAppliedIndexToDB.Load())
	}
	st.log.WithFields(logrus.Fields{
		"log_type":        l.Type,
		"log_name":        l.Type.String(),
		"log_index":       l.Index,
		"cmd_type":        cmd.Type,
		"cmd_type_name":   cmd.Type.String(),
		"cmd_class":       cmd.Class,
		"cmd_schema_only": schemaOnly,
	}).Debug("server.apply")

	f := func() {}

	switch cmd.Type {

	case api.ApplyRequest_TYPE_ADD_CLASS:
		f = func() {
			ret.Error = st.schemaManager.AddClass(&cmd, st.cfg.NodeID, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_RESTORE_CLASS:
		f = func() {
			ret.Error = st.schemaManager.RestoreClass(&cmd, st.cfg.NodeID, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_UPDATE_CLASS:
		f = func() {
			ret.Error = st.schemaManager.UpdateClass(&cmd, st.cfg.NodeID, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_DELETE_CLASS:
		f = func() {
			ret.Error = st.schemaManager.DeleteClass(&cmd, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_ADD_PROPERTY:
		f = func() {
			ret.Error = st.schemaManager.AddProperty(&cmd, schemaOnly, !catchingUp)
		}

	case api.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:
		f = func() {
			ret.Error = st.schemaManager.UpdateShardStatus(&cmd, schemaOnly)
		}
	case api.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD:
		f = func() {
			ret.Error = st.schemaManager.AddReplicaToShard(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_ADD_TENANT:
		f = func() {
			ret.Error = st.schemaManager.AddTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_UPDATE_TENANT:
		f = func() {
			ret.Error = st.schemaManager.UpdateTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_DELETE_TENANT:
		f = func() {
			ret.Error = st.schemaManager.DeleteTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_TENANT_PROCESS:
		f = func() {
			ret.Error = st.schemaManager.UpdateTenantsProcess(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_STORE_SCHEMA_V1:
		f = func() {
			ret.Error = st.StoreSchemaV1()
		}
	case api.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS:
		f = func() {
			ret.Error = st.authZManager.UpsertRolesPermissions(&cmd)
		}
	case api.ApplyRequest_TYPE_DELETE_ROLES:
		f = func() {
			ret.Error = st.authZManager.DeleteRoles(&cmd)
		}
	case api.ApplyRequest_TYPE_REMOVE_PERMISSIONS:
		f = func() {
			ret.Error = st.authZManager.RemovePermissions(&cmd)
		}
	case api.ApplyRequest_TYPE_ADD_ROLES_FOR_USER:
		f = func() {
			ret.Error = st.authZManager.AddRolesForUser(&cmd)
		}
	case api.ApplyRequest_TYPE_REVOKE_ROLES_FOR_USER:
		f = func() {
			ret.Error = st.authZManager.RevokeRolesForUser(&cmd)
		}

	case api.ApplyRequest_TYPE_UPSERT_USER:
		f = func() {
			ret.Error = st.dynUserManager.CreateUser(&cmd)
		}
	case api.ApplyRequest_TYPE_DELETE_USER:
		f = func() {
			ret.Error = st.dynUserManager.DeleteUser(&cmd)
		}
	case api.ApplyRequest_TYPE_ROTATE_USER_API_KEY:
		f = func() {
			ret.Error = st.dynUserManager.RotateKey(&cmd)
		}
	case api.ApplyRequest_TYPE_SUSPEND_USER:
		f = func() {
			ret.Error = st.dynUserManager.SuspendUser(&cmd)
		}
	case api.ApplyRequest_TYPE_ACTIVATE_USER:
		f = func() {
			ret.Error = st.dynUserManager.ActivateUser(&cmd)
		}

	case api.ApplyRequest_TYPE_REPLICATION_REPLICATE:
		f = func() {
			ret.Error = st.replicationManager.Replicate(l.Index, &cmd)
		}
	case api.ApplyRequest_TYPE_REPLICATION_REPLICATE_UPDATE_STATE:
		f = func() {
			ret.Error = st.replicationManager.UpdateReplicateOpState(&cmd)
		}

	default:
		// This could occur when a new command has been introduced in a later app version
		// At this point, we need to panic so that the app undergo an upgrade during restart
		const msg = "consider upgrading to newer version"
		st.log.WithFields(logrus.Fields{
			"type":  cmd.Type,
			"class": cmd.Class,
			"more":  msg,
		}).Error("unknown command")
	}

	// Wrap the function in a go routine to ensure panic recovery. This is necessary as this function is run in an
	// unwrapped goroutine in the raft library
	wg := sync.WaitGroup{}
	wg.Add(1)
	g := func() {
		f()
		wg.Done()
	}
	enterrors.GoWrapper(g, st.log)
	wg.Wait()

	return ret
}
