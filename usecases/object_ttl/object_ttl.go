//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objectttl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/ttl"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

var (
	// ErrAborted is the cause LocalStatus.Abort cancels with.
	ErrAborted = errors.New("aborted")
	// ErrFinished is the cause LocalStatus.Finished cancels with.
	ErrFinished = errors.New("finished")
)

// ErrStoppedEarly marks a sweep that was cancelled rather than one that failed,
// so a caller deciding a log level or a status code does not have to re-derive
// that judgement.
var ErrStoppedEarly = errors.New("deletion stopped early")

// MaxReportedErrors caps how many errors a sweep renders into one log record.
// A sweep collects an error per collection, shard and tenant it touches, so
// without a cap one record carries all of them.
const MaxReportedErrors = 10

// stoppedBy reports what stopped a sweep: its own context (an abort) or the
// caller's (shutdown, schedule change), or nil if neither did. It reads the
// contexts directly rather than a callee's error, since a wrapped error can't
// reliably say whether cancellation was involved.
func stoppedBy(callerCtx, sweepCtx context.Context) error {
	if cause := context.Cause(sweepCtx); cause != nil {
		return cause
	}
	return context.Cause(callerCtx)
}

type objectTTLAndVersion struct {
	version   uint64
	ttlConfig *models.ObjectTTLConfig
}

func NewCoordinator(schemaReader schemaUC.SchemaReader, schemaGetter schemaUC.SchemaGetter, db *db.DB,
	logger logrus.FieldLogger, clusterClient *http.Client, nodeResolver nodeResolver, localStatus *LocalStatus,
) *Coordinator {
	return &Coordinator{
		schemaReader:     schemaReader,
		schemaGetter:     schemaGetter,
		logger:           logger,
		clusterClient:    clusterClient,
		nodeResolver:     nodeResolver,
		db:               db,
		objectTTLOngoing: atomic.Bool{},
		remoteObjectTTL:  newRemoteObjectTTL(clusterClient, nodeResolver),
		localStatus:      localStatus,
	}
}

type Coordinator struct {
	schemaReader      schemaUC.SchemaReader
	schemaGetter      schemaUC.SchemaGetter
	db                *db.DB
	objectTTLOngoing  atomic.Bool
	logger            logrus.FieldLogger
	objectTTLLastNode string
	clusterClient     *http.Client
	nodeResolver      nodeResolver
	remoteObjectTTL   *remoteObjectTTL
	localStatus       *LocalStatus
}

// Start triggers the deletion of expired objects.
//
// It is expected to be called periodically, e.g., via a cron job on the RAFT Leader to ensure that there are no
// parallel executions running. The RAFT leader will send a request to a remote node in multi-node clusters as the
// coordinator of the next deletion run to not add any additional load on the leader. In single-node clusters, it will
// execute the deletion locally.
//
// There should always only one deletion run ongoing at any time. In case of remote deletions it will check with the last
// node used for deletion if the previous run is still ongoing and skip the current run if so.
func (c *Coordinator) Start(ctx context.Context, targetOwnNode bool, ttlTime, deletionTime time.Time) error {
	if !c.objectTTLOngoing.CompareAndSwap(false, true) {
		return fmt.Errorf("TTL deletion already ongoing")
	}
	defer c.objectTTLOngoing.Store(false)

	// gather classes with TTL enabled
	classesWithTTL := map[string]objectTTLAndVersion{}
	err := c.schemaReader.ReadSchema(func(class models.Class, version uint64) {
		if !ttl.IsTtlEnabled(class.ObjectTTLConfig) {
			return
		}
		classesWithTTL[class.Class] = objectTTLAndVersion{version: version, ttlConfig: class.ObjectTTLConfig}
	})
	if err != nil {
		return fmt.Errorf("schemareader: %w", err)
	}
	if len(classesWithTTL) == 0 {
		return nil
	}

	localNode := c.schemaGetter.NodeName()
	allNodes := c.schemaGetter.Nodes()
	remoteNodes := make([]string, 0, len(allNodes))
	remoteNodeSelected := ""

	if targetOwnNode {
		remoteNodes = append(remoteNodes, localNode)
	} else {
		for _, node := range allNodes {
			if node != localNode {
				remoteNodes = append(remoteNodes, node)
			}
		}
	}

	remoteNodesCount := len(remoteNodes)
	switch remoteNodesCount {
	case 0:
		// nothing to select
	case 1:
		remoteNodeSelected = remoteNodes[0]
	default:
		i := rand.Intn(remoteNodesCount)
		remoteNodeSelected = remoteNodes[i]
	}

	c.logger.WithFields(logrus.Fields{
		"action":        "objects_ttl_deletion",
		"all_nodes":     allNodes,
		"selected_node": remoteNodeSelected,
		"ttl_time":      ttlTime,
		"deletion_time": deletionTime,
	}).Debug("ttl deletion running")

	if remoteNodeSelected == "" {
		err = c.triggerDeletionObjectsExpiredLocalNode(ctx, classesWithTTL, ttlTime, deletionTime)
	} else {
		err = c.triggerDeletionObjectsExpiredRemoteNode(ctx, classesWithTTL, ttlTime, deletionTime, remoteNodeSelected)
	}
	// each arm marks its own: the local one knows the sweep's context, and the
	// remote one only ever sees the dispatch, because the peer answers 202
	// before it sweeps
	return err
}

// IsRunning reports whether this node still holds its deletion slot. An abort
// cancels the deletion but leaves the slot reserved until it returns, so this is
// what separates a node that is draining from one that is idle.
func (c *Coordinator) IsRunning() bool {
	return c.localStatus.IsRunning()
}

func (c *Coordinator) Abort(ctx context.Context, targetOwnNode bool) (bool, error) {
	localNode := c.schemaGetter.NodeName()
	allNodes := c.schemaGetter.Nodes()

	var remoteNodes []string
	if !targetOwnNode {
		remoteNodes = make([]string, 0, len(allNodes))
		for _, node := range allNodes {
			if node != localNode {
				remoteNodes = append(remoteNodes, node)
			}
		}
	}

	localAborted := c.localStatus.Abort()

	// abort just on local node
	if targetOwnNode || len(remoteNodes) == 0 {
		c.logger.WithFields(logrus.Fields{
			"action":  "objects_ttl_deletion",
			"aborted": localAborted,
			"node":    localNode,
		}).Warn("abort ttl deletion on local node")
		return localAborted, nil
	}

	// abort also on all remote nodes
	ec := errorcompounder.NewSafe()
	eg := enterrors.NewErrorGroupWrapper(c.logger)
	eg.SetLimit(concurrency.TimesFloatGOMAXPROCS(c.db.GetConfig().ObjectsTTLConcurrencyFactor.Get()))

	abortedNodes := make(map[string]bool, len(remoteNodes)+1)
	abortedNodes[localNode] = localAborted
	anyAborted := localAborted
	abortedLock := new(sync.Mutex)

	for _, nodeName := range remoteNodes {
		eg.Go(func() error {
			aborted, err := c.remoteObjectTTL.AbortRemoteDelete(ctx, nodeName)
			if err != nil {
				ec.AddGroups(err, nodeName)
			}
			abortedLock.Lock()
			anyAborted = anyAborted || aborted
			abortedNodes[nodeName] = aborted
			abortedLock.Unlock()
			return nil
		}, nodeName)
	}
	eg.WaitAndCollect(ec)
	err := ec.ToErrorLimited(MaxReportedErrors)

	l := c.logger.WithFields(logrus.Fields{
		"action":  "objects_ttl_deletion",
		"aborted": anyAborted,
		"nodes":   abortedNodes,
	})
	if err != nil {
		l.Warnf("abort ttl deletion on all nodes: %v", err)
	} else {
		l.Warn("abort ttl deletion on all nodes")
	}

	return anyAborted, err
}

func (c *Coordinator) triggerDeletionObjectsExpiredLocalNode(ctx context.Context, classesWithTTL map[string]objectTTLAndVersion,
	ttlTime, deletionTime time.Time,
) (err error) {
	ok, ttlCtx := c.localStatus.SetRunning()
	if !ok {
		return fmt.Errorf("another request is still being processed")
	}
	defer c.localStatus.Finished()

	started := time.Now()

	metrics := monitoring.GetMetrics()
	metrics.IncObjectsTtlCount()
	metrics.IncObjectsTtlRunning()

	defer monitoring.GetBackgroundProcessMetrics().Started(monitoring.ProcessTTLDeletion)()

	// count objects deleted per collection
	objsDeletedCounters := make(DeletedCounters, len(classesWithTTL))
	colNames := make([]string, 0, len(classesWithTTL))
	for colName := range classesWithTTL {
		colNames = append(colNames, colName)
	}

	ec := errorcompounder.NewSafe()
	dispatched := 0

	logger := c.logger.WithField("action", "objects_ttl_deletion")
	logger.WithFields(logrus.Fields{
		"collections":       colNames,
		"collections_count": len(colNames),
	}).Debug("ttl deletion on local node started")
	defer func() {
		took := time.Since(started)

		// add fields c_{collection_name}=>{count_deleted} and total_deleted=>{total_deleted}
		fields, total := objsDeletedCounters.ToLogFields(16)
		fields["took"] = took.String()
		logger = logger.WithFields(fields)

		metrics.DecObjectsTtlRunning()
		metrics.ObserveObjectsTtlDuration(took)
		metrics.AddObjectsTtlObjectsDeleted(float64(total))

		// ec holds what a callee returned; the contexts say whether the sweep was
		// stopped. Neither is read off the other.
		cause := stoppedBy(ctx, ttlCtx)
		if cause != nil {
			logger = logger.WithFields(logrus.Fields{
				"stopped_early":          true,
				"collections_dispatched": dispatched,
			})
		}

		if !ec.Empty() {
			metrics.IncObjectsTtlFailureCount()
			logger.Errorf("ttl deletion on local node failed: %v", err)
			return
		}
		if cause != nil {
			logger.Warnf("ttl deletion on local node stopped early: %v", cause)
			return
		}
		logger.Debug("ttl deletion on local node finished")
	}()

	eg := enterrors.NewErrorGroupWrapper(c.logger)
	eg.SetLimit(concurrency.TimesFloatGOMAXPROCS(c.db.GetConfig().ObjectsTTLConcurrencyFactor.Get()))

	for name, collection := range classesWithTTL {
		// the collections already dispatched run on ttlCtx, which this
		// cancellation does not reach, so they finish on their own
		if context.Cause(ctx) != nil {
			break
		}
		dispatched++
		// captured by value: a delete goroutine indexing objsDeletedCounters by
		// name would race the next iteration's map write, which is fatal.
		counter := &atomic.Int32{}
		objsDeletedCounters[name] = counter
		countDeleted := func(count int32) { counter.Add(count) }
		deleteOnPropName, ttlThreshold := c.extractTtlDataFromCollection(collection.ttlConfig, ttlTime)
		c.db.DeleteExpiredObjects(ttlCtx, eg, ec, name, deleteOnPropName, ttlThreshold, deletionTime, countDeleted, collection.version)
	}

	eg.WaitAndCollect(ec)

	// a failure outranks a stop: something broke whether or not anyone also
	// asked the sweep to end
	if err := ec.ToErrorLimited(MaxReportedErrors); err != nil {
		return fmt.Errorf("deletion of expired objects on local node: %w", err)
	}
	if cause := stoppedBy(ctx, ttlCtx); cause != nil {
		return fmt.Errorf("%w: %w", ErrStoppedEarly, cause)
	}
	return nil
}

func (c *Coordinator) triggerDeletionObjectsExpiredRemoteNode(ctx context.Context, classesWithTTL map[string]objectTTLAndVersion,
	ttlTime, deletionTime time.Time, node string,
) (err error) {
	started := time.Now()

	l := c.logger.WithFields(logrus.Fields{
		"action": "objects_ttl_deletion",
		"node":   node,
	})
	l.Debug("ttl deletion on remote node started")
	defer func() {
		l = l.WithField("took", time.Since(started))

		// this arm dispatches; the peer sweeps on a context of its own, so the
		// only stop this can observe is the caller giving up on the dispatch
		cause := context.Cause(ctx)
		if cause != nil {
			l = l.WithField("stopped_early", true)
		}

		if err != nil {
			l.Errorf("ttl deletion on remote node failed: %v", err)
			return
		}
		if cause != nil {
			l.Warnf("ttl deletion on remote node stopped early: %v", cause)
			return
		}
		l.Debug("ttl deletion on remote node finished")
	}()

	// check if deletion is running on the last node we picked
	if c.objectTTLLastNode != "" {
		l := l.WithField("last_node", c.objectTTLLastNode)

		ttlOngoing, err := c.remoteObjectTTL.CheckIfStillRunning(ctx, c.objectTTLLastNode)
		if err != nil {
			l.Errorf("Checking objectTTL running status failed: %v", err)
			// proceed with deletion
		} else if ttlOngoing {
			l.Warn("ObjectTTL is still running, skipping this round")
			return nil // deletion for collection still running, skip this round
		}
	}

	ttlCollections := make([]ObjectsExpiredPayload, 0, len(classesWithTTL))
	for name, collection := range classesWithTTL {
		deleteOnPropName, ttlThreshold := c.extractTtlDataFromCollection(collection.ttlConfig, ttlTime)

		ttlCollections = append(ttlCollections, ObjectsExpiredPayload{
			Class:        name,
			ClassVersion: collection.version,
			Prop:         deleteOnPropName,
			TtlMilli:     ttlThreshold.UnixMilli(),
			DelMilli:     deletionTime.UnixMilli(),
		})
	}

	c.objectTTLLastNode = node
	return c.remoteObjectTTL.StartRemoteDelete(ctx, node, ttlCollections)
}

func (c *Coordinator) extractTtlDataFromCollection(ttlConfig *models.ObjectTTLConfig, ttlTime time.Time,
) (string, time.Time) {
	deleteOnPropName := ttlConfig.DeleteOn
	ttlThreshold := ttlTime.Add(-time.Second * time.Duration(ttlConfig.DefaultTTL))
	return deleteOnPropName, ttlThreshold
}

type remoteObjectTTL struct {
	client       *http.Client
	nodeResolver nodeResolver
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

func newRemoteObjectTTL(httpClient *http.Client, nodeResolver nodeResolver) *remoteObjectTTL {
	return &remoteObjectTTL{client: httpClient, nodeResolver: nodeResolver}
}

func (c *remoteObjectTTL) CheckIfStillRunning(ctx context.Context, nodeName string) (bool, error) {
	p := "/cluster/object_ttl/status"
	method := http.MethodGet
	hostName, found := c.nodeResolver.NodeHostname(nodeName)
	if !found {
		return false, fmt.Errorf("unable to resolve hostname for %s", nodeName)
	}
	url := url.URL{Scheme: "http", Host: hostName, Path: p}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return false, enterrors.NewErrOpenHttpRequest(err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return false, enterrors.NewErrSendHttpRequest(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return false, enterrors.NewErrUnexpectedStatusCode(res.StatusCode, body)
	}

	var stillRunning ObjectsExpiredStatusResponse
	err = json.Unmarshal(body, &stillRunning)
	if err != nil {
		return false, enterrors.NewErrUnmarshalBody(err)
	}

	if ct, ok := stillRunning.CheckContentTypeHeader(res); !ok {
		return false, enterrors.NewErrUnexpectedContentType(ct)
	}

	return stillRunning.DeletionOngoing, nil
}

func (c *remoteObjectTTL) StartRemoteDelete(ctx context.Context, nodeName string, classes []ObjectsExpiredPayload) error {
	p := "/cluster/object_ttl/delete_expired"
	method := http.MethodPost
	hostName, found := c.nodeResolver.NodeHostname(nodeName)
	if !found {
		return fmt.Errorf("unable to resolve hostname for %s", nodeName)
	}
	url := url.URL{Scheme: "http", Host: hostName, Path: p}

	jsonBody, err := json.Marshal(classes)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, url.String(), bytes.NewBuffer(jsonBody))
	if err != nil {
		return enterrors.NewErrOpenHttpRequest(err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return enterrors.NewErrSendHttpRequest(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusAccepted {
		return enterrors.NewErrUnexpectedStatusCode(res.StatusCode, body)
	}

	return nil
}

func (c *remoteObjectTTL) AbortRemoteDelete(ctx context.Context, nodeName string) (bool, error) {
	p := "/cluster/object_ttl/abort"
	hostName, found := c.nodeResolver.NodeHostname(nodeName)
	if !found {
		return false, fmt.Errorf("unable to resolve hostname for %s", nodeName)
	}

	method := http.MethodPost
	url := url.URL{Scheme: "http", Host: hostName, Path: p}
	req, err := http.NewRequestWithContext(ctx, method, url.String(), nil)
	if err != nil {
		return false, enterrors.NewErrOpenHttpRequest(err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return false, enterrors.NewErrSendHttpRequest(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return false, enterrors.NewErrUnexpectedStatusCode(res.StatusCode, body)
	}

	var abortedResponse ObjectsExpiredAbortResponse
	err = json.Unmarshal(body, &abortedResponse)
	if err != nil {
		return false, enterrors.NewErrUnmarshalBody(err)
	}

	if ct, ok := abortedResponse.CheckContentTypeHeader(res); !ok {
		return false, enterrors.NewErrUnexpectedContentType(ct)
	}

	return abortedResponse.Aborted, nil
}

type DeletedCounters map[string]*atomic.Int32

func (dc DeletedCounters) ToLogFields(maxCollectionNameLen int) (fields logrus.Fields, total int32) {
	prefixLen := maxCollectionNameLen / 2
	suffixLen := maxCollectionNameLen - 1 - prefixLen
	shorten := func(name string) string {
		if ln := len(name); ln > maxCollectionNameLen {
			return name[:prefixLen] + "*" + name[ln-suffixLen:]
		}
		return name
	}

	fields = logrus.Fields{}
	total = int32(0)
	for name, counter := range dc {
		if del := counter.Load(); del > 0 {
			fields["c_"+shorten(name)] = del
			total += del
		}
	}
	fields["total_deleted"] = total
	return fields, total
}

// ----------------------------------------------------------------------------

// LocalStatus keeps status of ongoing TTL deletion on local node.
// isRunning is set to true when TTL deletion start and reset when finishes.
// Status is global per node. Only one deletion can run at a time, following requests
// to start new deletion should be rejected until ongoing one finishes.
// When running flag is set new context is created to be passed to started process.
// Context can be cancelled by abort call, which should eventually stop ongoing deletion.
// Abort call do not change isRunning flag. It is changed when deletion is actually finished,
// as context can be verified with delay.
type LocalStatus struct {
	lock          *sync.Mutex
	isRunning     bool
	runningCtx    context.Context
	runningCancel context.CancelCauseFunc
}

func NewLocalStatus() *LocalStatus {
	return &LocalStatus{
		lock:      new(sync.Mutex),
		isRunning: false,
	}
}

func (s *LocalStatus) IsRunning() bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.isRunning
}

func (s *LocalStatus) SetRunning() (success bool, ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isRunning {
		return false, nil
	}

	s.isRunning = true
	s.runningCtx, s.runningCancel = context.WithCancelCause(context.Background())
	return true, s.runningCtx
}

// Abort cancels the deletion holding the slot and reports whether there was
// one to cancel. It keeps reporting true while that deletion drains, because
// the coordinator ORs the answer across nodes — a false midway would read as
// "nothing happened" for a cluster still aborting.
func (s *LocalStatus) Abort() (aborted bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.isRunning {
		return false
	}

	s.runningCancel(enterrors.NewCanceledCause(ErrAborted))
	return true
}

// Finished releases the slot for the next deletion. Cancelling is what stops a
// deletion still reading the context; an already-cancelled one keeps the cause
// it was cancelled with.
func (s *LocalStatus) Finished() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.isRunning {
		return
	}

	s.runningCancel(enterrors.NewCanceledCause(ErrFinished))

	s.isRunning = false
	s.runningCtx, s.runningCancel = nil, nil
}
