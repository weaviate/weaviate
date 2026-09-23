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
	"github.com/weaviate/weaviate/usecases/namespaces"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// ErrDeletionStopped marks a deletion that ended because its context did: an
// abort, a schedule change or a server shutdown. A stop is not a failure, so it
// is logged at Warn and left out of the failure count.
var ErrDeletionStopped = errors.New("ttl deletion stopped")

// DeletionResult is the error a deletion that ran on ctx ends with, given the
// errors it collected. If ctx has ended by then, the errors make it a stop and
// the stop carries them, since most are the stop reaching a collection and the
// next run retries the rest. A deletion that finished clean before it noticed
// ctx ending succeeded.
func DeletionResult(ctx context.Context, collected error) error {
	if collected == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("%w (%w): %w", ErrDeletionStopped, cause, collected)
	}
	return collected
}

type objectTTLAndVersion struct {
	version   uint64
	ttlConfig *models.ObjectTTLConfig
}

func NewCoordinator(schemaReader schemaUC.SchemaReader, schemaGetter schemaUC.SchemaGetter,
	namespacesExister namespaces.Exister, db *db.DB, logger logrus.FieldLogger,
	clusterClient *http.Client, nodeResolver nodeResolver, localStatus *LocalStatus,
) *Coordinator {
	return &Coordinator{
		schemaReader:      schemaReader,
		schemaGetter:      schemaGetter,
		namespacesExister: namespacesExister,
		logger:            logger,
		clusterClient:     clusterClient,
		nodeResolver:      nodeResolver,
		db:                db,
		objectTTLOngoing:  atomic.Bool{},
		remoteObjectTTL:   newRemoteObjectTTL(clusterClient, nodeResolver),
		localStatus:       localStatus,
	}
}

type Coordinator struct {
	schemaReader      schemaUC.SchemaReader
	schemaGetter      schemaUC.SchemaGetter
	namespacesExister namespaces.Exister
	db                *db.DB
	objectTTLOngoing  atomic.Bool
	logger            logrus.FieldLogger
	objectTTLLastNode string
	clusterClient     *http.Client
	nodeResolver      nodeResolver
	remoteObjectTTL   *remoteObjectTTL
	localStatus       *LocalStatus
}

// Start triggers the deletion of expired objects. Collections whose namespace is
// not active are left untouched, see dropClassesWithoutActiveNamespace.
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
	c.dropClassesWithoutActiveNamespace(classesWithTTL)
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
		return c.triggerDeletionObjectsExpiredLocalNode(ctx, classesWithTTL, ttlTime, deletionTime)
	}
	return c.triggerDeletionObjectsExpiredRemoteNode(ctx, classesWithTTL, ttlTime, deletionTime, remoteNodeSelected)
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

	localAborted := c.localStatus.ResetRunning("aborted")

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
		})
	}
	eg.Wait()
	err := ec.ToError()

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
	ok, ttlCtx := c.localStatus.SetRunning(ctx)
	if !ok {
		return fmt.Errorf("another request is still being processed")
	}
	defer c.localStatus.FinishRunning(ttlCtx, "finished")

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

		switch {
		case errors.Is(err, ErrDeletionStopped):
			logger.Warnf("ttl deletion on local node stopped: %v", err)
		case err != nil:
			metrics.IncObjectsTtlFailureCount()
			logger.Errorf("ttl deletion on local node failed: %v", err)
		default:
			logger.Debug("ttl deletion on local node finished")
		}
	}()

	ec := errorcompounder.NewSafe()
	eg := enterrors.NewErrorGroupWrapper(c.logger)
	eg.SetLimit(concurrency.TimesFloatGOMAXPROCS(c.db.GetConfig().ObjectsTTLConcurrencyFactor.Get()))

	for name, collection := range classesWithTTL {
		if err := context.Cause(ttlCtx); err != nil {
			ec.Add(err)
			break
		}
		objsDeletedCounters[name] = &atomic.Int32{}
		countDeleted := func(count int32) { objsDeletedCounters[name].Add(count) }
		deleteOnPropName, ttlThreshold := c.extractTtlDataFromCollection(collection.ttlConfig, ttlTime)
		c.db.DeleteExpiredObjects(ttlCtx, eg, ec, name, deleteOnPropName, ttlThreshold, deletionTime, countDeleted, collection.version)
	}

	eg.Wait() // ignore errors from eg as they are already collected in ec

	err = DeletionResult(ttlCtx, ec.ToError())
	if err != nil && !errors.Is(err, ErrDeletionStopped) {
		return fmt.Errorf("deletion of expired objects on local node: %w", err)
	}
	return err
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
		switch {
		case errors.Is(err, ErrDeletionStopped):
			l.Warnf("ttl deletion on remote node stopped: %v", err)
		case err != nil:
			l.Errorf("ttl deletion on remote node failed: %v", err)
		default:
			l.Debug("ttl deletion on remote node finished")
		}
	}()

	// check if deletion is running on the last node we picked
	if c.objectTTLLastNode != "" {
		l := l.WithField("last_node", c.objectTTLLastNode)

		ttlOngoing, err := c.remoteObjectTTL.CheckIfStillRunning(ctx, c.objectTTLLastNode)
		switch {
		case err != nil && ctx.Err() != nil:
			return DeletionResult(ctx, err)
		case err != nil:
			l.Errorf("Checking objectTTL running status failed: %v", err)
			// proceed with deletion
		case ttlOngoing:
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
	// A stop that cuts the request short may land after the node took the
	// deletion on, so the deletion can still run there.
	return DeletionResult(ctx, c.remoteObjectTTL.StartRemoteDelete(ctx, node, ttlCollections))
}

// dropClassesWithoutActiveNamespace removes every collection whose namespace is
// not active. Sweeping one only counts a failure per round, since the shard load
// it reaches refuses on exactly the states RequireActive refuses. Kept out of the
// ReadSchema callback so the namespace lock is not taken under the schema read.
func (c *Coordinator) dropClassesWithoutActiveNamespace(classesWithTTL map[string]objectTTLAndVersion) {
	for name := range classesWithTTL {
		if err := namespaces.RequireActive(c.namespacesExister, namespacing.NamespaceFromQualified(name)); err != nil {
			delete(classesWithTTL, name)
		}
	}
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
// SetRunning derives the deletion's context from the caller's, so the deletion
// stops when that context ends or when ResetRunning is called on abort. The
// caller's context ending leaves isRunning set until the deletion finishes and
// calls FinishRunning, since the deletion notices the cancel with a delay. An
// abort clears isRunning at once, so a new deletion can start while the aborted
// one winds down, and FinishRunning leaves the new one's status alone.
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

func (s *LocalStatus) SetRunning(ctx context.Context) (success bool, runningCtx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isRunning {
		return false, nil
	}

	s.isRunning = true
	s.runningCtx, s.runningCancel = context.WithCancelCause(ctx)
	return true, s.runningCtx
}

// ResetRunning ends whichever deletion is running, as an abort does.
func (s *LocalStatus) ResetRunning(cause string) (success bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.isRunning {
		return false
	}
	s.reset(cause)
	return true
}

// FinishRunning ends the deletion SetRunning handed runningCtx to, and does
// nothing once an abort has ended it, since the status may belong to a newer
// deletion by then.
func (s *LocalStatus) FinishRunning(runningCtx context.Context, cause string) (success bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !s.isRunning || s.runningCtx != runningCtx {
		return false
	}
	s.reset(cause)
	return true
}

func (s *LocalStatus) reset(cause string) {
	s.runningCancel(enterrors.NewCanceledCause(cause))

	s.isRunning = false
	s.runningCtx, s.runningCancel = nil, nil
}
