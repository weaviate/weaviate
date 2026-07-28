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
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema"
)

const (
	DefaultTelemetryConsumerURL = "aHR0cHM6Ly90ZWxlbWV0cnkud2Vhdmlh" +
		"dGUuaW8vd2VhdmlhdGUtdGVsZW1ldHJ5"
	DefaultTelemetryPushInterval = 24 * time.Hour
)

type nodesStatusGetter interface {
	LocalNodeStatus(ctx context.Context, className, shardName, output string) *models.NodeStatus
}

// Telemeter is responsible for managing the transmission of telemetry data
type Telemeter struct {
	machineID          strfmt.UUID
	nodesStatusGetter  nodesStatusGetter
	schemaManager      schema.SchemaGetter
	logger             logrus.FieldLogger
	shutdown           chan struct{}
	failedToStart      bool
	consumer           string
	pushInterval       time.Duration
	clientTracker      *ClientTracker
	integrationTracker *IntegrationTracker
	cloudInfoHelper    *cloudInfoHelper

	// nodeID is CLUSTER_HOSTNAME (the raft node name): a stable per-node identity.
	nodeID string
	// clusterID is the raft-committed cluster identity, read on every push.
	clusterID            func() string
	asyncIndexingEnabled bool
}

// Config holds the scalar settings for a Telemeter.
type Config struct {
	// ConsumerURL is base64-encoded. If empty, DefaultTelemetryConsumerURL is used.
	ConsumerURL string
	// PushInterval defaults to DefaultTelemetryPushInterval if zero.
	PushInterval time.Duration
	// Enabled gates whether usage trackers spin up and payloads are pushed.
	Enabled bool
	// NodeID is CLUSTER_HOSTNAME (the raft node name): a stable per-node identity.
	NodeID string
	// AsyncIndexingEnabled is wired from server config.
	AsyncIndexingEnabled bool
	// ClusterID returns the UUID committed once per cluster lifetime via raft. It is
	// called on every push and MUST be non-nil (return "" until the id is committed).
	ClusterID func() string
}

// New creates a new Telemeter instance.
func New(nodesStatusGetter nodesStatusGetter, schemaManager schema.SchemaGetter,
	logger logrus.FieldLogger, consumerURL string, pushInterval time.Duration,
	telemetryEnabled bool,
	cfg Config,
) *Telemeter {
	if consumerURL == "" {
		consumerURL = DefaultTelemetryConsumerURL
	}
	if pushInterval == 0 {
		pushInterval = DefaultTelemetryPushInterval
	}

	tel := &Telemeter{
		// machineID is a fresh UUID per process, distinct from nodeID.
		machineID:            strfmt.UUID(uuid.NewString()),
		nodesStatusGetter:    nodesStatusGetter,
		schemaManager:        schemaManager,
		logger:               logger,
		shutdown:             make(chan struct{}),
		consumer:             consumerURL,
		pushInterval:         pushInterval,
		clientTracker:        NewClientTracker(logger),
		cloudInfoHelper:      newCloudInfoHelper(logger, cfg.Enabled),
		nodeID:               cfg.NodeID,
		asyncIndexingEnabled: cfg.AsyncIndexingEnabled,
		clusterID:            cfg.ClusterID,
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

	cf := tel.curatedFields()
	asyncIndexing := tel.asyncIndexingEnabled

	return &Payload{
		MachineID:                  tel.machineID,
		Type:                       payloadType,
		Version:                    config.ServerVersion,
		ObjectsCount:               objs,
		OS:                         runtime.GOOS,
		Arch:                       runtime.GOARCH,
		UsedModules:                usedMods,
		CollectionsCount:           cols,
		ClientUsage:                clientUsage,
		ClientIntegrationUsage:     clientIntegrationUsage,
		CloudProvider:              cloudProvider,
		UniqueID:                   uniqueID,
		NodeID:                     tel.nodeID,
		ClusterID:                  tel.clusterID(),
		NodeCount:                  &cf.nodeCount,
		MaxReplicationFactor:       &cf.maxReplicationFactor,
		ReplicationEnabled:         &cf.replicationEnabled,
		MTCollectionCount:          &cf.mtCollectionCount,
		NamedVectorCollectionCount: &cf.namedVectorCount,
		AsyncIndexingEnabled:       &asyncIndexing,
		VectorIndexTypeCounts:      cf.vectorIndexTypeCounts,
	}, nil
}

// curatedFields holds the schema-derived telemetry signal fields.
type curatedFields struct {
	nodeCount             int
	maxReplicationFactor  int
	replicationEnabled    bool
	mtCollectionCount     int
	namedVectorCount      int
	vectorIndexTypeCounts map[string]int
}

// curatedFields extracts the schema-derived signal fields in a single schema pass.
func (tel *Telemeter) curatedFields() curatedFields {
	cf := curatedFields{nodeCount: len(tel.schemaManager.Nodes())}

	sch := tel.schemaManager.GetSchemaSkipAuth()
	if sch.Objects == nil {
		return cf
	}

	counts := make(map[string]int)
	for _, class := range sch.Objects.Classes {
		if class == nil {
			continue
		}
		if rf := tel.replicationFactor(class); rf > cf.maxReplicationFactor {
			cf.maxReplicationFactor = rf
		}
		if class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled {
			cf.mtCollectionCount++
		}
		if tel.countVectorIndexTypes(class, counts) {
			cf.namedVectorCount++
		}
	}

	cf.replicationEnabled = cf.maxReplicationFactor > 1
	if len(counts) > 0 {
		cf.vectorIndexTypeCounts = counts
	}
	return cf
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

// replicationFactor returns the class replication factor, or 0 if unset.
func (tel *Telemeter) replicationFactor(class *models.Class) int {
	if class.ReplicationConfig == nil {
		return 0
	}
	return int(class.ReplicationConfig.Factor)
}

// countVectorIndexTypes records the vector index type(s) for one class into counts
// (defaulting "" to "hnsw") and reports whether the class uses named vectors.
func (tel *Telemeter) countVectorIndexTypes(class *models.Class, counts map[string]int) (named bool) {
	if len(class.VectorConfig) > 0 {
		for _, vc := range class.VectorConfig {
			counts[tel.orHNSW(vc.VectorIndexType)]++
		}
		return true
	}
	// legacy single-vector: read from class-level VectorIndexType
	counts[tel.orHNSW(class.VectorIndexType)]++
	return false
}

// orHNSW maps an empty vector index type to the "hnsw" default.
func (tel *Telemeter) orHNSW(vectorIndexType string) string {
	if vectorIndexType == "" {
		return "hnsw"
	}
	return vectorIndexType
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
				// Fallback for classes with nil ModuleConfig but a set class-level
				// Vectorizer (pre-v1.14 schemas). "none" (BYOV) is excluded - not a module.
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
