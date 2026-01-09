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
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/ttl"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type objectTTLAndVersion struct {
	version   uint64
	ttlConfig *models.ObjectTTLConfig
}

func NewCoordinator(schemaReader schemaUC.SchemaReader, schemaGetter schemaUC.SchemaGetter, db *db.DB, logger logrus.FieldLogger, clusterClient *http.Client, nodeResolver nodeResolver) *Coordinator {
	return &Coordinator{
		schemaReader:     schemaReader,
		schemaGetter:     schemaGetter,
		logger:           logger,
		clusterClient:    clusterClient,
		nodeResolver:     nodeResolver,
		db:               db,
		objectTTLOngoing: atomic.Bool{},
		remoteObjectTTL:  newRemoteObjectTTL(clusterClient, nodeResolver),
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
		return c.triggerDeletionObjectsExpiredLocalNode(ctx, classesWithTTL, ttlTime, deletionTime)
	}
	return c.triggerDeletionObjectsExpiredRemoteNode(ctx, classesWithTTL, ttlTime, deletionTime, remoteNodeSelected)
}

func (c *Coordinator) triggerDeletionObjectsExpiredLocalNode(ctx context.Context, classesWithTTL map[string]objectTTLAndVersion,
	ttlTime, deletionTime time.Time,
) (err error) {
	started := time.Now()
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
	}).Info("ttl deletion on local node started")
	defer func() {
		// add fields c_{collection_name}=>{count_deleted} and total_deleted=>{total_deleted}
		fields := objsDeletedCounters.ToLogFields(16)
		fields["took"] = time.Since(started).String()
		logger = logger.WithFields(fields)

		if err != nil {
			logger.WithError(err).Error("ttl deletion on local node failed")
			return
		}
		logger.Info("ttl deletion on local node finished")
	}()

	ec := errorcompounder.NewSafe()
	eg := enterrors.NewErrorGroupWrapper(c.logger)
	eg.SetLimit(concurrency.TimesFloatGOMAXPROCS(c.db.GetConfig().ObjectsTTLConcurrencyFactor.Get()))

	for name, collection := range classesWithTTL {
		if err := ctx.Err(); err != nil {
			break
		}
		objsDeletedCounters[name] = &atomic.Int32{}
		countDeleted := func(count int32) { objsDeletedCounters[name].Add(count) }
		deleteOnPropName, ttlThreshold := c.extractTtlDataFromCollection(collection.ttlConfig, ttlTime)
		c.db.DeleteExpiredObjects(ctx, eg, ec, name, deleteOnPropName, ttlThreshold, deletionTime, countDeleted, collection.version)
	}

	eg.Wait() // ignore errors from eg as they are already collected in ec

	if err := ec.ToError(); err != nil {
		return fmt.Errorf("deletion of expired objects on local node: %w", err)
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
	l.Info("ttl deletion on remote node started")
	defer func() {
		l = l.WithField("took", time.Since(started))
		if err != nil {
			l.WithError(err).Error("ttl deletion on remote node failed")
			return
		}
		l.Info("ttl deletion on remote node finished")
	}()

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

	// check if deletion is running on the last node we picked
	if c.objectTTLLastNode != "" {
		l := l.WithField("last_node", c.objectTTLLastNode)

		ttlOngoing, err := c.remoteObjectTTL.CheckIfStillRunning(ctx, c.objectTTLLastNode)
		if err != nil {
			l.Errorf("Checking objectTTL running status failed: %v", err)
		} else if ttlOngoing {
			l.Warn("ObjectTTL is still running, skipping this round")
			return nil // deletion for collection still running, skip this round
		}
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

type DeletedCounters map[string]*atomic.Int32

func (dc DeletedCounters) ToLogFields(maxCollectionNameLen int) logrus.Fields {
	prefixLen := maxCollectionNameLen / 2
	suffixLen := maxCollectionNameLen - 1 - prefixLen
	shorten := func(name string) string {
		if ln := len(name); ln > maxCollectionNameLen {
			return name[:prefixLen] + "*" + name[ln-suffixLen:]
		}
		return name
	}

	fields := logrus.Fields{}
	total := int32(0)
	for name, counter := range dc {
		if del := counter.Load(); del > 0 {
			fields["c_"+shorten(name)] = del
			total += del
		}
	}
	fields["total_deleted"] = total
	return fields
}
