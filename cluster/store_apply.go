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
	"errors"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"google.golang.org/protobuf/proto"
	gproto "google.golang.org/protobuf/proto"
)

func (st *Store) Execute(req *api.ApplyRequest) (uint64, error) {
	st.log.WithFields(logrus.Fields{
		"type":  req.Type,
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
		// If the current node is not the leader (it might have changed recently) return ErrNotLeader to ensure that we
		// will retry the apply to the leader
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, types.ErrNotLeader
		}
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
	st.log.WithFields(logrus.Fields{
		"log_type":  l.Type,
		"log_name":  l.Type.String(),
		"log_index": l.Index,
	}).Debug("apply fsm store command")
	if l.Type != raft.LogCommand {
		st.log.WithFields(logrus.Fields{
			"type":  l.Type,
			"index": l.Index,
		}).Info("not a valid command")
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
	schemaOnly := st.lastAppliedIndexOnStart.Load() != 0 && l.Index <= st.lastAppliedIndexOnStart.Load()
	defer func() {
		// If we have an applied index from the previous store (i.e from disk). Then reload the DB once we catch up as
		// that means we're done doing schema only.
		if st.lastAppliedIndexOnStart.Load() != 0 && l.Index == st.lastAppliedIndexOnStart.Load() {
			st.log.WithFields(logrus.Fields{
				"log_type":                     l.Type,
				"log_name":                     l.Type.String(),
				"log_index":                    l.Index,
				"last_store_log_applied_index": st.lastAppliedIndexOnStart.Load(),
			}).Debug("reloading local DB as RAFT and local DB are now caught up")
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
	st.log.WithFields(logrus.Fields{
		"log_type":        l.Type,
		"log_name":        l.Type.String(),
		"log_index":       l.Index,
		"cmd_type":        cmd.Type,
		"cmd_type_name":   cmd.Type.String(),
		"cmd_class":       cmd.Class,
		"cmd_schema_only": schemaOnly,
	}).Debug("server.apply")

	var f func()

	switch cmd.Type {

	case api.ApplyRequest_TYPE_ADD_CLASS:
		f = func() {
			ret.Error = st.schemaManager.AddClass(&cmd, st.cfg.NodeID, schemaOnly)
		}

	case api.ApplyRequest_TYPE_RESTORE_CLASS:
		f = func() {
			ret.Error = st.schemaManager.RestoreClass(&cmd, st.cfg.NodeID, schemaOnly)
		}

	case api.ApplyRequest_TYPE_UPDATE_CLASS:
		f = func() {
			ret.Error = st.schemaManager.UpdateClass(&cmd, st.cfg.NodeID, schemaOnly)
		}

	case api.ApplyRequest_TYPE_DELETE_CLASS:
		f = func() {
			ret.Error = st.schemaManager.DeleteClass(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_ADD_PROPERTY:
		f = func() {
			ret.Error = st.schemaManager.AddProperty(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_UPDATE_SHARD_STATUS:
		f = func() {
			ret.Error = st.schemaManager.UpdateShardStatus(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_ADD_TENANT:
		f = func() {
			ret.Error = st.schemaManager.AddTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_UPDATE_TENANT:
		f = func() {
			ret.Data, ret.Error = st.schemaManager.UpdateTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_DELETE_TENANT:
		f = func() {
			ret.Error = st.schemaManager.DeleteTenants(&cmd, schemaOnly)
		}

	case api.ApplyRequest_TYPE_STORE_SCHEMA_V1:
		f = func() {
			ret.Error = st.StoreSchemaV1()
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
		panic(fmt.Sprintf("unknown command type=%d class=%s more=%s", cmd.Type, cmd.Class, msg))
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
