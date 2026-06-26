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

package telemetry

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	DefaultTelemetryConsumerURL = "aHR0cHM6Ly90ZWxlbWV0cnkud2Vhdmlh" +
		"dGUuaW8vd2VhdmlhdGUtdGVsZW1ldHJ5"
	DefaultTelemetryPushInterval = 24 * time.Hour
)

type nodesStatusGetter interface {
	LocalNodeStatus(ctx context.Context, className, shardName, output string) *models.NodeStatus
}

type schemaManager interface {
	GetSchemaSkipAuth() schema.Schema
	// Nodes returns a snapshot of the current cluster node names.
	// concrete *schema.Handler already implements this.
	Nodes() []string
}

// Telemeter is responsible for managing the transmission of telemetry data
type Telemeter struct {
	machineID          strfmt.UUID
	nodesStatusGetter  nodesStatusGetter
	schemaManager      schemaManager
	logger             logrus.FieldLogger
	shutdown           chan struct{}
	failedToStart      bool
	consumer           string
	pushInterval       time.Duration
	clientTracker      *ClientTracker
	integrationTracker *IntegrationTracker
	cloudInfoHelper    *cloudInfoHelper

	// nodeID is the persisted per-node UUID from the data-volume file. NOT the hostname.
	// machineID stays uuid.NewString() per process (unchanged, counts restarts).
	nodeID string
	// clusterID and clusterCreatedAt are populated in Start() before the Init push.
	clusterID           string
	clusterCreatedAt    int64
	asyncIndexingEnabled bool
	// waitForClusterID blocks until the raft leader commits the cluster identity.
	waitForClusterID func(ctx context.Context) (string, int64, error)
}

// New creates a new Telemeter instance.
// consumerURL should be base64-encoded. If empty, uses DefaultTelemetryConsumerURL.
// pushInterval defaults to DefaultTelemetryPushInterval if zero.
// nodeID is the persisted per-node UUID from the data-volume file (NOT the hostname).
// asyncIndexingEnabled is wired from server config.
// waitForClusterID blocks until the raft leader commits the cluster identity; may be nil
// (e.g. in single-node mode) in which case clusterId is omitted from payloads.
func New(nodesStatusGetter nodesStatusGetter, schemaManager schemaManager,
	logger logrus.FieldLogger, consumerURL string, pushInterval time.Duration,
	telemetryEnabled bool,
	nodeID string,
	asyncIndexingEnabled bool,
	waitForClusterID func(ctx context.Context) (string, int64, error),
) *Telemeter {
	if consumerURL == "" {
		consumerURL = DefaultTelemetryConsumerURL
	}
	if pushInterval == 0 {
		pushInterval = DefaultTelemetryPushInterval
	}

	tel := &Telemeter{
		// machineID is a fresh UUID each process start - this is intentional and unchanged.
		// It counts restarts. nodeID (below) is the stable per-node identity.
		machineID:            strfmt.UUID(uuid.NewString()),
		nodesStatusGetter:    nodesStatusGetter,
		schemaManager:        schemaManager,
		logger:               logger,
		shutdown:             make(chan struct{}),
		consumer:             consumerURL,
		pushInterval:         pushInterval,
		cloudInfoHelper:      newCloudInfoHelper(logger, telemetryEnabled),
		nodeID:               nodeID,
		asyncIndexingEnabled: asyncIndexingEnabled,
		waitForClusterID:     waitForClusterID,
	}
	// Only spin up tracker goroutines when telemetry is enabled; otherwise they
	// would leak for the lifetime of the process, since shutdown only calls
	// Stop when telemetry is enabled. Callers must handle a nil tracker
	// (middleware and debug handlers already do).
	if telemetryEnabled {
		tel.clientTracker = NewClientTracker(logger)
		tel.integrationTracker = NewIntegrationTracker(logger)
	}
	return tel
}

// ReadOrCreateNodeID reads the stable per-node UUID from dataPath/node-id.
// If the file does not exist it generates a new UUID, writes it atomically
// (tmp+rename mirroring adapters/repos/db/inverted/new_prop_length_tracker.go),
// and returns it. On any I/O error it returns an error; the caller falls back
// to an ephemeral UUID so a read-only data dir does not prevent startup.
func ReadOrCreateNodeID(dataPath string) (string, error) {
	nodePath := filepath.Join(dataPath, "node-id")

	// happy path: file exists
	if b, err := os.ReadFile(nodePath); err == nil {
		id := strings.TrimSpace(string(b))
		if id != "" {
			return id, nil
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read node-id: %w", err)
	}

	// generate a fresh UUID and persist it atomically
	id := uuid.NewString()
	tmp := nodePath + ".tmp"
	if err := os.WriteFile(tmp, []byte(id), 0o666); err != nil {
		return "", fmt.Errorf("write node-id tmp: %w", err)
	}
	if err := os.Rename(tmp, nodePath); err != nil {
		return "", fmt.Errorf("rename node-id: %w", err)
	}
	// re-read to confirm (mirrors new_prop_length_tracker pattern)
	b, err := os.ReadFile(nodePath)
	if err != nil {
		return "", fmt.Errorf("re-read node-id after write: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// GetClientTracker returns the client tracker instance for use in middleware
func (tel *Telemeter) GetClientTracker() *ClientTracker {
	return tel.clientTracker
}

// GetIntegrationTracker returns the integration tracker instance for use in middleware
func (tel *Telemeter) GetIntegrationTracker() *IntegrationTracker {
	return tel.integrationTracker
}

// Start begins telemetry for the node
func (tel *Telemeter) Start(ctx context.Context) error {
	// Wait up to 30 s for the raft leader to commit the cluster identity before the
	// INIT push. On timeout we proceed without clusterId (best-effort; do not block
	// startup). Single-node Docker typically resolves within ~1-2 s; restarts with a
	// snapshot have the id restored immediately.
	if tel.waitForClusterID != nil {
		waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		clusterID, clusterCreatedAt, err := tel.waitForClusterID(waitCtx)
		if err != nil {
			tel.logger.WithField("action", "telemetry_cluster_id_wait").
				Warnf("cluster identity not available within 30s, pushing INIT without clusterId: %v", err)
		} else {
			tel.clusterID = clusterID
			tel.clusterCreatedAt = clusterCreatedAt
		}
	}

	payload, err := tel.push(ctx, PayloadType.Init)
	if err != nil {
		tel.failedToStart = true
		return fmt.Errorf("push: %w", err)
	}
	f := func() {
		t := time.NewTicker(tel.pushInterval)
		defer t.Stop()
		for {
			select {
			case <-tel.shutdown:
				return
			case <-t.C:
				payload, err = tel.push(ctx, PayloadType.Update)
				if err != nil {
					tel.logger.
						WithField("action", "telemetry_push").
						WithField("payload", fmt.Sprintf("%+v", payload)).
						WithField("retry_at", time.Now().Add(tel.pushInterval).Format(time.RFC3339)).
						Error(err.Error())
					continue
				}
				tel.logger.
					WithField("action", "telemetry_push").
					WithField("payload", fmt.Sprintf("%+v", payload)).
					Info("telemetry update")
			}
		}
	}
	enterrors.GoWrapper(f, tel.logger)

	tel.logger.
		WithField("action", "telemetry_push").
		WithField("payload", fmt.Sprintf("%+v", payload)).
		Info("telemetry started")
	return nil
}

// Stop shuts down the telemeter
func (tel *Telemeter) Stop(ctx context.Context) error {
	// Always stop the tracker goroutines, even if telemetry failed to start.
	// This prevents goroutine leaks.
	defer func() {
		if tel.clientTracker != nil {
			tel.clientTracker.Stop()
		}
		if tel.integrationTracker != nil {
			tel.integrationTracker.Stop()
		}
	}()

	if tel.failedToStart {
		return nil
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown telemetry: %w", ctx.Err())
	case tel.shutdown <- struct{}{}:
		payload, err := tel.push(ctx, PayloadType.Terminate)
		if err != nil {
			tel.logger.
				WithField("action", "telemetry_push").
				WithField("payload", fmt.Sprintf("%+v", payload)).
				Error(err.Error())
			return err
		}
		tel.logger.
			WithField("action", "telemetry_push").
			WithField("payload", fmt.Sprintf("%+v", payload)).
			Info("telemetry terminated")

		return nil
	}
}

// push sends telemetry data to the consumer url
func (tel *Telemeter) push(ctx context.Context, payloadType string) (*Payload, error) {
	payload, err := tel.buildPayload(ctx, payloadType)
	if err != nil {
		return nil, fmt.Errorf("build payload: %w", err)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	url, err := base64.StdEncoding.DecodeString(tel.consumer)
	if err != nil {
		return nil, fmt.Errorf("decode url: %w", err)
	}

	resp, err := http.Post(string(url), "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request unsuccessful, status code: %d, body: %s", resp.StatusCode, string(body))
	}
	return payload, nil
}

func (tel *Telemeter) buildPayload(ctx context.Context, payloadType string) (*Payload, error) {
	usedMods, err := tel.getUsedModules()
	if err != nil {
		return nil, fmt.Errorf("get used modules: %w", err)
	}

	var objs int64
	// The first payload should not include object count,
	// because all the shards may not be loaded yet. We
	// don't want to force load for telemetry alone
	if payloadType != PayloadType.Init {
		objs, err = tel.getObjectCount(ctx)
		if err != nil {
			return nil, fmt.Errorf("get object count: %w", err)
		}
	}

	cols, err := tel.getCollectionsCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("get collections count: %w", err)
	}

	// Get client usage data and reset for the next period.
	// For Init payloads, we don't have client data yet, so skip it.
	clientUsage, clientIntegrationUsage := tel.collectUsageForPayload(payloadType)

	cloudProvider, uniqueID := tel.getCloudInfo()

	// Curated signal fields - one pass over the schema.
	nodeCount, maxRF, mtCount, namedVecCount, vectorIndexTypeCounts := tel.getCuratedSchemaFields()

	return &Payload{
		MachineID:              tel.machineID,
		Type:                   payloadType,
		Version:                config.ServerVersion,
		ObjectsCount:           objs,
		OS:                     runtime.GOOS,
		Arch:                   runtime.GOARCH,
		UsedModules:            usedMods,
		CollectionsCount:       cols,
		ClientUsage:            clientUsage,
		ClientIntegrationUsage: clientIntegrationUsage,
		CloudProvider:          cloudProvider,
		UniqueID:               uniqueID,
		// stable identity
		NodeID:           tel.nodeID,
		ClusterID:        tel.clusterID,
		ClusterCreatedAt: tel.clusterCreatedAt,
		// curated signal
		NodeCount:                  nodeCount,
		MaxReplicationFactor:       maxRF,
		ReplicationEnabled:         maxRF > 1,
		MTCollectionCount:          mtCount,
		NamedVectorCollectionCount: namedVecCount,
		AsyncIndexingEnabled:       tel.asyncIndexingEnabled,
		VectorIndexTypeCounts:      vectorIndexTypeCounts,
	}, nil
}

// getCuratedSchemaFields performs a single pass over the schema to extract the
// curated signal fields. Returns (nodeCount, maxRF, mtCount, namedVecCount, vectorIndexTypeCounts).
func (tel *Telemeter) getCuratedSchemaFields() (nodeCount, maxRF, mtCount, namedVecCount int, vectorIndexTypeCounts map[string]int) {
	nodeCount = len(tel.schemaManager.Nodes())

	sch := tel.schemaManager.GetSchemaSkipAuth()
	if sch.Objects == nil {
		return nodeCount, 0, 0, 0, nil
	}

	vectorIndexTypeCounts = make(map[string]int)

	for _, class := range sch.Objects.Classes {
		if class == nil {
			continue
		}

		// replication factor
		if class.ReplicationConfig != nil {
			rf := int(class.ReplicationConfig.Factor)
			if rf > maxRF {
				maxRF = rf
			}
		}

		// multi-tenancy
		if class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled {
			mtCount++
		}

		// named-vector collections
		if len(class.VectorConfig) > 0 {
			namedVecCount++
			for _, vc := range class.VectorConfig {
				vit := vc.VectorIndexType
				if vit == "" {
					vit = "hnsw"
				}
				vectorIndexTypeCounts[vit]++
			}
		} else {
			// legacy single-vector: read from class-level VectorIndexType
			vit := class.VectorIndexType
			if vit == "" {
				vit = "hnsw"
			}
			vectorIndexTypeCounts[vit]++
		}
	}

	if len(vectorIndexTypeCounts) == 0 {
		vectorIndexTypeCounts = nil
	}
	return nodeCount, maxRF, mtCount, namedVecCount, vectorIndexTypeCounts
}

// collectUsageForPayload returns client and integration usage maps for the given
// payload type, resetting the trackers. Returns (nil, nil) for Init payloads,
// and nil for any map that contains no data.
func (tel *Telemeter) collectUsageForPayload(payloadType string) (
	clientUsage map[ClientType]map[string]int64,
	clientIntegrationUsage map[string]map[string]int64,
) {
	if payloadType == PayloadType.Init {
		return nil, nil
	}
	if tel.clientTracker != nil {
		clientUsage = tel.clientTracker.GetAndReset()
		if len(clientUsage) == 0 {
			clientUsage = nil
		}
	}
	if tel.integrationTracker != nil {
		clientIntegrationUsage = tel.integrationTracker.GetAndReset()
		if len(clientIntegrationUsage) == 0 {
			clientIntegrationUsage = nil
		}
	}
	return clientUsage, clientIntegrationUsage
}

func (tel *Telemeter) getUsedModules() ([]string, error) {
	sch := tel.schemaManager.GetSchemaSkipAuth()
	usedModulesMap := map[string]struct{}{}

	if sch.Objects != nil {
		for _, class := range sch.Objects.Classes {
			if class == nil {
				continue
			}
			if modCfg, ok := class.ModuleConfig.(map[string]interface{}); ok {
				for name, cfg := range modCfg {
					usedModulesMap[tel.determineModule(name, cfg)] = struct{}{}
				}
			} else if class.Vectorizer != "" && class.Vectorizer != "none" {
				// Fallback for classes whose ModuleConfig is nil but the class-level
				// Vectorizer field is set (pre-v1.14 schemas and old-migration gap).
				// "none" (BYOV) is intentionally excluded - it is not a module.
				usedModulesMap[class.Vectorizer] = struct{}{}
			}
			for _, vectorConfig := range class.VectorConfig {
				if modCfg, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok {
					for name, cfg := range modCfg {
						usedModulesMap[tel.determineModule(name, cfg)] = struct{}{}
					}
				}
			}
		}
	}

	var usedModules []string
	for modName := range usedModulesMap {
		usedModules = append(usedModules, modName)
	}
	sort.Strings(usedModules)
	return usedModules, nil
}

func (tel *Telemeter) determineModule(name string, cfg interface{}) string {
	if strings.Contains(name, "palm") || strings.Contains(name, "google") {
		if settings, ok := cfg.(map[string]interface{}); ok {
			if apiEndpoint, ok := settings["apiEndpoint"]; ok {
				if apiEndpointStr, ok := apiEndpoint.(string); ok && apiEndpointStr == "generativelanguage.googleapis.com" {
					return fmt.Sprintf("%s-ai-studio", strings.Replace(name, "palm", "google", 1))
				}
			}
		}
		return fmt.Sprintf("%s-vertex-ai", strings.Replace(name, "palm", "google", 1))
	}
	return name
}

func (tel *Telemeter) getObjectCount(ctx context.Context) (int64, error) {
	status := tel.nodesStatusGetter.LocalNodeStatus(ctx, "", "", verbosity.OutputVerbose)
	if status == nil || status.Stats == nil {
		return 0, fmt.Errorf("received nil node stats")
	}
	return status.Stats.ObjectCount, nil
}

func (tel *Telemeter) getCollectionsCount(context.Context) (int, error) {
	sch := tel.schemaManager.GetSchemaSkipAuth()
	if sch.Objects == nil {
		return 0, nil
	}
	return len(sch.Objects.Classes), nil
}

func (tel *Telemeter) getCloudInfo() (cloudProvider, uniqueID *string) {
	if ci := tel.cloudInfoHelper.getCloudInfo(); ci != nil {
		if ci.cloudProvider != "" {
			cloudProvider = &ci.cloudProvider
		}
		if ci.uniqueID != "" {
			uniqueID = &ci.uniqueID
		}
	}
	return
}
