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

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/config"
)

// BackupTaskProvider implements the DTM Provider interfaces for backup creation.
type BackupTaskProvider struct {
	mu       sync.Mutex
	node     string
	logger   logrus.FieldLogger
	cfg      config.Backup
	sourcer  Sourcer
	rbacSrc  RBACSnapshotter
	userSrc  dynUserSnapshotter
	backends BackupBackendProvider

	recorder distributedtask.TaskCompletionRecorder

	// nodeHandler holds the node-wide lastOp latch shared with the legacy 2PC
	// path. Only one backup (DTM or 2PC) runs per node at a time.
	// Nil in provider-only unit tests.
	nodeHandler *Handler

	// appliedIndexProbe returns nil when the local store has replayed past
	// the given raft index, and an error otherwise.
	appliedIndexProbe func(ctx context.Context, version uint64) error

	// dataPath is the DB data root. CleanupTask scans it on cache miss.
	dataPath string

	// payloadCache maps backup ID to raw payload bytes. StartTask fills,
	// CleanupTask clears. OnTaskCompleted reads the task record because
	// bootstrap may have already cleared this cache.
	payloadCache map[string][]byte

	activeHandles map[string]*backupTaskHandle
}

type BackupTaskProviderParams struct {
	Node              string
	Logger            logrus.FieldLogger
	Cfg               config.Backup
	Sourcer           Sourcer
	RBACSrc           RBACSnapshotter
	UserSrc           dynUserSnapshotter
	Backends          BackupBackendProvider
	NodeHandler       *Handler
	AppliedIndexProbe func(ctx context.Context, version uint64) error
	DataPath          string
}

func NewBackupTaskProvider(p BackupTaskProviderParams) *BackupTaskProvider {
	return &BackupTaskProvider{
		node:              p.Node,
		logger:            p.Logger,
		cfg:               p.Cfg,
		sourcer:           p.Sourcer,
		rbacSrc:           p.RBACSrc,
		userSrc:           p.UserSrc,
		backends:          p.Backends,
		nodeHandler:       p.NodeHandler,
		appliedIndexProbe: p.AppliedIndexProbe,
		dataPath:          p.DataPath,
		payloadCache:      make(map[string][]byte),
		activeHandles:     make(map[string]*backupTaskHandle),
	}
}

// --- Provider interface ---

func (p *BackupTaskProvider) SetCompletionRecorder(recorder distributedtask.TaskCompletionRecorder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recorder = recorder
}

// GetLocalTasks reports cached descriptors so the scheduler's bootstrap
// CleanupTask path fires for backup tasks.
func (p *BackupTaskProvider) GetLocalTasks() []distributedtask.TaskDescriptor {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.payloadCache) == 0 {
		return nil
	}
	descs := make([]distributedtask.TaskDescriptor, 0, len(p.payloadCache))
	for id := range p.payloadCache {
		descs = append(descs, distributedtask.TaskDescriptor{ID: id})
	}
	return descs
}

// CleanupTask releases local backup state. It is safe to call twice and safe
// mid-flight, because it never touches the descriptor in the object store.
// When the payload cache misses, it scans the staging directories instead.
func (p *BackupTaskProvider) CleanupTask(desc distributedtask.TaskDescriptor) error {
	p.mu.Lock()
	payload, ok := p.payloadCache[desc.ID]
	delete(p.payloadCache, desc.ID)
	delete(p.activeHandles, desc.ID)
	p.mu.Unlock()

	var classes []string
	if ok {
		if tp, err := unmarshalTaskPayload(payload); err == nil {
			if nd, exists := tp.Nodes[p.node]; exists {
				classes = nd.Classes
			}
		}
	}

	// After bootstrap the payload cache is empty; scan staging roots instead.
	if len(classes) == 0 && p.sourcer != nil && p.dataPath != "" {
		classes = p.scanStagingClasses(desc.ID)
	}

	for _, cls := range classes {
		if err := p.sourcer.ReleaseBackup(context.Background(), desc.ID, cls); err != nil {
			p.logger.WithField("class", cls).WithField("backup_id", desc.ID).
				Warnf("CleanupTask: failed to release backup: %v", err)
		}
	}

	p.logger.WithField("backup_id", desc.ID).Info("CleanupTask: released local backup state")
	return nil
}

// scanStagingClasses finds classes with a staging directory for backupID.
// Layout: <dataPath>/<class>/<shard>/<backupID>.
func (p *BackupTaskProvider) scanStagingClasses(backupID string) []string {
	if p.dataPath == "" {
		return nil
	}
	var classes []string
	seen := map[string]bool{}

	classEntries, err := os.ReadDir(p.dataPath)
	if err != nil {
		return nil
	}
	for _, classEntry := range classEntries {
		if !classEntry.IsDir() {
			continue
		}
		classPath := filepath.Join(p.dataPath, classEntry.Name())
		shardEntries, err := os.ReadDir(classPath)
		if err != nil {
			continue
		}
		for _, shardEntry := range shardEntries {
			if !shardEntry.IsDir() {
				continue
			}
			stagingDir := filepath.Join(classPath, shardEntry.Name(), backupID)
			if info, err := os.Stat(stagingDir); err == nil && info.IsDir() {
				className := classEntry.Name()
				if !seen[className] {
					seen[className] = true
					classes = append(classes, className)
				}
			}
		}
	}
	return classes
}

func (p *BackupTaskProvider) StartTask(task *distributedtask.Task) (distributedtask.TaskHandle, error) {
	payload, err := unmarshalTaskPayload(task.Payload)
	if err != nil {
		return nil, fmt.Errorf("start backup task: %w", err)
	}

	if p.appliedIndexProbe != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.appliedIndexProbe(ctx, task.Version)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("backup %s: local store not caught up to task version %d: %w",
				task.ID, task.Version, err)
		}
	}

	localGroup, hasLocal := payload.Nodes[p.node]
	if !hasLocal || len(localGroup.Classes) == 0 {
		return newIdleTaskHandle(), nil
	}

	p.mu.Lock()
	if h, running := p.activeHandles[task.ID]; running {
		p.mu.Unlock()
		return h, nil
	}
	recorder := p.recorder
	p.mu.Unlock()

	// Acquire the node-wide latch. If another backup (2PC or DTM) holds it,
	// the local units fail rather than running two backups concurrently.
	if slot := p.opSlot(); slot != nil {
		if prevID := slot.renew(task.ID, p.nodeHomeDir(payload), payload.Bucket, payload.Path); prevID != "" {
			if prevID != task.ID {
				msg := fmt.Sprintf("node %s is already running backup %q", p.node, prevID)
				p.failAllUnits(task, payload, recorder, msg, false)
				return nil, fmt.Errorf("backup %s: %s", task.ID, msg)
			}
			// The latch holds this ID but there is no local handle, so another
			// flow in this process owns it. Idle rather than start a second one.
			p.logger.WithField("backup_id", task.ID).
				Info("StartTask: latch already holds this backup id, re-attaching idle")
			return newIdleTaskHandle(), nil
		}
	}

	handle := newBackupTaskHandle(task.ID)

	p.mu.Lock()
	p.payloadCache[task.ID] = task.Payload
	p.activeHandles[task.ID] = handle
	p.mu.Unlock()

	enterrors.GoWrapper(func() {
		defer handle.markDone()
		defer p.releaseNodeSlot(task.ID)
		p.runNodeBackup(handle, task, payload, localGroup.Classes, recorder)
	}, p.logger)

	return handle, nil
}

// opSlot is the node-wide backup latch, nil when no node handler is wired.
func (p *BackupTaskProvider) opSlot() *backupStat {
	if p.nodeHandler == nil || p.nodeHandler.backupper == nil {
		return nil
	}
	return &p.nodeHandler.backupper.lastOp
}

// nodeHomeDir returns the path the latch reports for the operation. It returns
// empty when the backend cannot be resolved. The flow then fails on the same
// backend and reports that failure.
func (p *BackupTaskProvider) nodeHomeDir(payload *taskPayload) string {
	store, err := nodeBackend(p.node, p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
	if err != nil {
		return ""
	}
	return store.HomeDir(payload.Bucket, payload.Path)
}

// releaseNodeSlot drops the node's active-flow state once the flow returns. It
// releases the latch only while the latch still holds this backup's ID, so a
// flow that outlived its latch cannot free another operation's latch.
func (p *BackupTaskProvider) releaseNodeSlot(taskID string) {
	p.mu.Lock()
	delete(p.activeHandles, taskID)
	p.mu.Unlock()

	if slot := p.opSlot(); slot != nil && slot.get().ID == taskID {
		slot.reset()
	}
}

// --- UnitAwareProvider interface ---

func (p *BackupTaskProvider) OnGroupCompleted(task *distributedtask.Task, groupID string, localGroupUnitIDs []string) error {
	return nil
}

func (p *BackupTaskProvider) OnSwapRequested(task *distributedtask.Task, groupID string, localGroupUnitIDs []string) error {
	return nil
}

func (p *BackupTaskProvider) OnTaskCompleted(task *distributedtask.Task) error {
	payload, err := unmarshalTaskPayload(task.Payload)
	if err != nil {
		return fmt.Errorf("OnTaskCompleted: %w", err)
	}
	if _, hasLocal := payload.Nodes[p.node]; !hasLocal {
		return nil
	}
	return p.writeTerminalDescriptor(task, payload)
}

// --- ConflictDetector interface ---

func (p *BackupTaskProvider) CheckConflict(newPayload []byte, existingTasks []*distributedtask.Task) error {
	np, err := unmarshalTaskPayload(newPayload)
	if err != nil {
		return fmt.Errorf("CheckConflict: unmarshal new payload: %w", err)
	}
	for _, t := range existingTasks {
		if t.ID == np.ID {
			return fmt.Errorf("backup %q already exists (status %s)", np.ID, t.Status)
		}
		if t.Status.IsActive() {
			return fmt.Errorf("backup %q is already in progress; only one backup at a time is allowed", t.ID)
		}
	}
	return nil
}

// --- CrossNamespaceConflictDetector interface ---

func (p *BackupTaskProvider) CheckCrossNamespaceConflict(newPayload []byte, allTasks map[string]map[string]*distributedtask.Task) error {
	for ns, tasks := range allTasks {
		if ns == BackupTaskNamespace {
			continue
		}
		for id, t := range tasks {
			if t.Status.IsActive() {
				return fmt.Errorf("cannot start backup while %s task %q is active", ns, id)
			}
		}
	}
	return nil
}

// --- CompletedTaskRetainer interface ---

func (p *BackupTaskProvider) ShouldRetainCompletedTask(task *distributedtask.Task, namespaceTasks map[distributedtask.TaskDescriptor]*distributedtask.Task) bool {
	payload, err := unmarshalTaskPayload(task.Payload)
	if err != nil {
		return true
	}
	store, err := coordBackend(p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
	if err != nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	meta, err := store.Meta(ctx, GlobalBackupFile, payload.Bucket, payload.Path)
	if err != nil {
		return true
	}
	return !isFinalStatus(meta.Status)
}

// --- TerminalCleanupProvider interface ---

func (p *BackupTaskProvider) TerminalCleanupDone(task *distributedtask.Task, localNode string) bool {
	payload, err := unmarshalTaskPayload(task.Payload)
	if err != nil {
		return false
	}
	store, err := coordBackend(p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	meta, err := store.Meta(ctx, GlobalBackupFile, payload.Bucket, payload.Path)
	if err != nil {
		return false
	}
	return isFinalStatus(meta.Status)
}

// --- internal execution ---

// keepaliveInterval is how often unit progress is re-reported to prevent the
// stale detector from killing slow uploads.
const keepaliveInterval = 5 * time.Second

// descriptorWriteAttempts caps retries when writing the global descriptor.
const descriptorWriteAttempts = 3

func (p *BackupTaskProvider) runNodeBackup(
	handle *backupTaskHandle,
	task *distributedtask.Task,
	payload *taskPayload,
	classes []string,
	recorder distributedtask.TaskCompletionRecorder,
) {
	logFields := logrus.Fields{
		"action":    "dtm_backup",
		"backup_id": task.ID,
		"node":      p.node,
	}

	store, err := nodeBackend(p.node, p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
	if err != nil {
		p.logger.WithFields(logFields).Errorf("failed to init backend: %v", err)
		p.failAllUnits(task, payload, recorder, err.Error(), false)
		return
	}

	if err := store.Initialize(context.Background(), payload.Bucket, payload.Path); err != nil {
		p.logger.WithFields(logFields).Errorf("failed to initialize backend: %v", err)
		p.failAllUnits(task, payload, recorder, err.Error(), false)
		return
	}

	compressionType := payload.CompressionType

	ctx, cancel := context.WithCancel(context.Background())
	handle.setCancelFunc(cancel)
	defer cancel()

	// Claim units: progress=0 transitions PENDING to IN_PROGRESS.
	for _, cls := range classes {
		unitID := fmt.Sprintf("%s/%s", p.node, cls)
		if err := recorder.UpdateDistributedTaskUnitProgress(
			ctx, BackupTaskNamespace, task.ID, task.Version, p.node, unitID, 0,
		); err != nil {
			p.logger.WithFields(logFields).WithField("unit", unitID).
				Warnf("claim write failed: %v", err)
		}
	}

	keepaliveCtx, keepaliveCancel := context.WithCancel(ctx)
	defer keepaliveCancel()
	enterrors.GoWrapper(func() {
		p.runKeepalive(keepaliveCtx, task, classes, recorder)
	}, p.logger)

	// Every unit-owning node writes the global descriptor at flow start. The
	// first write wins, and every writer produces the same content.
	if err := p.writeStartedDescriptor(ctx, task, payload); err != nil {
		p.logger.WithFields(logFields).Errorf("write started descriptor: %v", err)
		p.failAllUnits(task, payload, recorder, err.Error(), false)
		return
	}

	baseBackupID := payload.BaseBackupID
	baseDescrs, err := resolveBaseBackupChain(ctx, baseBackupID, time.Now().UTC(), payload.Bucket, payload.Path, compressionType, store.MetaForBackupID)
	if err != nil {
		if !errors.As(err, &backup.ErrNotFound{}) {
			p.logger.WithFields(logFields).Errorf("resolve base backup chain: %v", err)
			p.failAllUnits(task, payload, recorder, err.Error(), false)
			return
		}
		p.logger.WithFields(logFields).Warn("node not present in base backup, uploading full backup for this node")
		baseDescrs = nil
		baseBackupID = ""
	}

	desc := backup.BackupDescriptor{
		StartedAt:       task.StartedAt,
		ID:              task.ID,
		Classes:         make([]backup.ClassDescriptor, 0, len(classes)),
		Version:         Version,
		ServerVersion:   payload.ServerVersion,
		CompressionType: &compressionType,
		BaseBackupID:    baseBackupID,
	}

	// The node-wide latch doubles as the uploader's status publisher, so the
	// legacy node status endpoint reports a DTM backup exactly as it reports a
	// 2PC one.
	var slot statusPublisher = &noopStatusPublisher{}
	if s := p.opSlot(); s != nil {
		slot = s
	}
	up := newUploader(p.cfg, p.sourcer, p.rbacSrc, p.userSrc, payload.Users, payload.Roles, store, task.ID, slot, p.logger).
		withCompression(newZipConfig(payload.Compression))

	// Called from uploader.all's per-class loop in backend.go.
	var completedClassesMu sync.Mutex
	completedClasses := make([]string, 0, len(classes))
	up.onClassUploaded = func(className string) {
		completedClassesMu.Lock()
		completedClasses = append(completedClasses, className)
		completedClassesMu.Unlock()

		// The last unit is held back until the node descriptor is written.
		completedClassesMu.Lock()
		n := len(completedClasses)
		completedClassesMu.Unlock()

		if n < len(classes) {
			unitID := fmt.Sprintf("%s/%s", p.node, className)
			if err := recorder.RecordDistributedTaskUnitCompletion(
				ctx, BackupTaskNamespace, task.ID, task.Version, p.node, unitID,
			); err != nil {
				if !errors.Is(err, distributedtask.ErrUnitAlreadyTerminal) &&
					!errors.Is(err, distributedtask.ErrTaskNotRunning) {
					p.logger.WithField("backup_id", task.ID).WithField("unit", unitID).
						Warnf("per-class unit completion failed: %v", err)
				}
			}
		}
	}

	p.logger.WithFields(logFields).Info("starting DTM backup upload")
	uploadErr := up.all(ctx, classes, &desc, baseDescrs, payload.Bucket, payload.Path)
	desc.CompletedAt = time.Now().UTC()

	keepaliveCancel()

	if uploadErr != nil {
		p.logger.WithFields(logFields).Errorf("DTM backup upload failed: %v", uploadErr)
		errMsg := uploadErr.Error()
		retryable := isRetryableUploadError(uploadErr)
		p.failAllUnits(task, payload, recorder, errMsg, retryable)
		return
	}

	p.logger.WithFields(logFields).Info("DTM backup upload completed")

	// Report the held-back last unit. The node descriptor was already
	// written by uploader.all's defer.
	if len(classes) > 0 {
		lastClass := classes[len(classes)-1]
		// The callback may not have fired for every class, so report any
		// non-last class that is still unreported.
		completedClassesMu.Lock()
		alreadyReported := make(map[string]bool, len(completedClasses))
		for _, c := range completedClasses {
			if c != lastClass {
				alreadyReported[c] = true
			}
		}
		completedClassesMu.Unlock()

		for _, cls := range classes {
			if cls == lastClass {
				continue
			}
			if alreadyReported[cls] {
				continue
			}
			unitID := fmt.Sprintf("%s/%s", p.node, cls)
			_ = recorder.RecordDistributedTaskUnitCompletion(
				ctx, BackupTaskNamespace, task.ID, task.Version, p.node, unitID,
			)
		}

		unitID := fmt.Sprintf("%s/%s", p.node, lastClass)
		if err := recorder.RecordDistributedTaskUnitCompletion(
			ctx, BackupTaskNamespace, task.ID, task.Version, p.node, unitID,
		); err != nil {
			if !errors.Is(err, distributedtask.ErrUnitAlreadyTerminal) &&
				!errors.Is(err, distributedtask.ErrTaskNotRunning) {
				p.logger.WithField("backup_id", task.ID).WithField("unit", unitID).
					Warnf("last unit completion failed: %v", err)
			}
		}
	}
}

// runKeepalive re-reports progress=0 for each local unit every tick so the
// stale detector's UpdatedAt check stays fresh during slow uploads.
func (p *BackupTaskProvider) runKeepalive(
	ctx context.Context,
	task *distributedtask.Task,
	classes []string,
	recorder distributedtask.TaskCompletionRecorder,
) {
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, cls := range classes {
				unitID := fmt.Sprintf("%s/%s", p.node, cls)
				// progress=0 skips the update throttle, and the FSM always
				// refreshes UpdatedAt.
				_ = recorder.UpdateDistributedTaskUnitProgress(
					ctx, BackupTaskNamespace, task.ID, task.Version, p.node, unitID, 0,
				)
			}
		}
	}
}

func (p *BackupTaskProvider) failAllUnits(
	task *distributedtask.Task,
	payload *taskPayload,
	recorder distributedtask.TaskCompletionRecorder,
	errMsg string,
	retryable bool,
) {
	localGroup, ok := payload.Nodes[p.node]
	if !ok {
		return
	}
	for _, cls := range localGroup.Classes {
		unitID := fmt.Sprintf("%s/%s", p.node, cls)
		var reportErr error
		if retryable {
			reportErr = recorder.RecordDistributedTaskRetryableUnitFailure(
				context.Background(), BackupTaskNamespace, task.ID, task.Version, p.node, unitID, errMsg,
			)
		} else {
			reportErr = recorder.RecordDistributedTaskUnitFailure(
				context.Background(), BackupTaskNamespace, task.ID, task.Version, p.node, unitID, errMsg,
			)
		}
		if reportErr != nil {
			if !errors.Is(reportErr, distributedtask.ErrUnitAlreadyTerminal) &&
				!errors.Is(reportErr, distributedtask.ErrTaskNotRunning) {
				p.logger.WithField("backup_id", task.ID).WithField("unit", unitID).
					Warnf("failed to report unit failure: %v", reportErr)
			}
		}
	}
}

// writeStartedDescriptor writes the global Started descriptor at flow start.
// All fields come from the payload and task record, so concurrent writers on
// different nodes produce identical content. An existing descriptor is left
// untouched: the first write wins and a terminal status never flips back.
func (p *BackupTaskProvider) writeStartedDescriptor(ctx context.Context, task *distributedtask.Task, payload *taskPayload) error {
	store, err := coordBackend(p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
	if err != nil {
		return fmt.Errorf("started descriptor: init backend: %w", err)
	}

	if _, err := store.Meta(ctx, GlobalBackupFile, payload.Bucket, payload.Path); err == nil {
		return nil
	} else if !errors.As(err, &backup.ErrNotFound{}) {
		return fmt.Errorf("started descriptor: read existing: %w", err)
	}

	descriptor := &backup.DistributedBackupDescriptor{
		StartedAt:       task.StartedAt,
		ID:              payload.ID,
		Nodes:           payload.Nodes,
		Version:         Version,
		ServerVersion:   payload.ServerVersion,
		Leader:          payload.Leader,
		BaseBackupID:    payload.BaseBackupID,
		Users:           payload.Users,
		Roles:           payload.Roles,
		CompressionType: payload.CompressionType,
		Status:          backup.Started,
	}

	var writeErr error
	for attempt := range descriptorWriteAttempts {
		writeErr = store.PutMeta(ctx, GlobalBackupFile, descriptor, payload.Bucket, payload.Path)
		if writeErr == nil {
			return nil
		}
		p.logger.WithField("backup_id", task.ID).WithField("attempt", attempt+1).
			Warnf("failed to write started descriptor: %v", writeErr)
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	return fmt.Errorf("write started descriptor after retries: %w", writeErr)
}

func (p *BackupTaskProvider) writeTerminalDescriptor(task *distributedtask.Task, payload *taskPayload) error {
	store, err := coordBackend(p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
	if err != nil {
		return fmt.Errorf("terminal descriptor: init backend: %w", err)
	}

	ctx := context.Background()

	existing, err := store.Meta(ctx, GlobalBackupFile, payload.Bucket, payload.Path)
	if err == nil && isFinalStatus(existing.Status) {
		return nil
	}

	status, errMsg := backupVerdict(task)

	descriptor := &backup.DistributedBackupDescriptor{
		StartedAt:     task.StartedAt,
		ID:            payload.ID,
		Nodes:         payload.Nodes,
		Version:       Version,
		ServerVersion: payload.ServerVersion,
		Leader:        payload.Leader,
		BaseBackupID:  payload.BaseBackupID,
		Users:         payload.Users,
		Roles:         payload.Roles,
		Status:        status,
		Error:         errMsg,
		CompletedAt:   time.Now().UTC(),
	}

	var totalSize int64
	for nodeName, nd := range payload.Nodes {
		ns, nsErr := nodeBackend(nodeName, p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
		if nsErr != nil {
			nd.Error = fmt.Sprintf("cannot init backend for node %s: %v", nodeName, nsErr)
			continue
		}
		meta, readErr := ns.Meta(ctx, payload.ID, payload.Bucket, payload.Path)
		if readErr != nil {
			if status != backup.Success {
				// Fill per-node status/error from the task record when the
				// node descriptor is missing (e.g. the node died before
				// writing it).
				fillNodeFromTaskRecord(nd, nodeName, task, readErr)
				continue
			}
			nd.Error = fmt.Sprintf("missing node descriptor for %s: %v", nodeName, readErr)
			continue
		}
		nd.PreCompressionSizeBytes = meta.PreCompressionSizeBytes
		totalSize += meta.PreCompressionSizeBytes
		nd.Status = meta.Status
	}
	descriptor.PreCompressionSizeBytes = totalSize
	descriptor.CompressionType = payload.CompressionType

	var writeErr error
	for attempt := range descriptorWriteAttempts {
		writeErr = store.PutMeta(ctx, GlobalBackupFile, descriptor, payload.Bucket, payload.Path)
		if writeErr == nil {
			return nil
		}
		p.logger.WithField("backup_id", task.ID).WithField("attempt", attempt+1).
			Warnf("failed to write terminal descriptor: %v", writeErr)
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	return fmt.Errorf("write terminal descriptor after retries: %w", writeErr)
}

func backupVerdict(task *distributedtask.Task) (backup.Status, string) {
	switch task.Status {
	case distributedtask.TaskStatusCancelled:
		return backup.Cancelled, "backup canceled"
	case distributedtask.TaskStatusFailed:
		return backup.Failed, collectTaskErrors(task)
	default:
		if task.AnyUnitFailed() {
			return backup.Failed, collectTaskErrors(task)
		}
		for _, ack := range task.PostCompletionAcks {
			if !ack.Success {
				return backup.Failed, ack.Error
			}
		}
		return backup.Success, ""
	}
}

// fillNodeFromTaskRecord fills a node's descriptor entry from the task record
// when the node's backup.json is unreadable on a failure path.
//   - definite not-found: synthesise status/error from unit data, prefixed with
//     "per task record:" so the source is visible. Sizes stay zero.
//   - any other read error: annotate only. A transient error must not overwrite
//     a durable node file that may exist.
func fillNodeFromTaskRecord(nd *backup.NodeDescriptor, nodeName string, task *distributedtask.Task, readErr error) {
	if !errors.As(readErr, &backup.ErrNotFound{}) {
		nd.Error = fmt.Sprintf("node descriptor unreadable: %v", readErr)
		return
	}

	// Definite not-found: the node never wrote its descriptor.
	var unitErrs []string
	allFailed := true
	for id, u := range task.Units {
		if !strings.HasPrefix(id, nodeName+"/") {
			continue
		}
		if u.Status != distributedtask.UnitStatusFailed {
			allFailed = false
		}
		if u.Error != "" {
			unitErrs = append(unitErrs, fmt.Sprintf("unit %s: %s", id, u.Error))
		}
	}

	if allFailed && len(unitErrs) > 0 {
		nd.Status = backup.Failed
		nd.Error = "per task record: " + strings.Join(unitErrs, "; ")
	} else if len(unitErrs) > 0 {
		nd.Error = "per task record: " + strings.Join(unitErrs, "; ")
	} else {
		nd.Error = fmt.Sprintf("per task record: node %s descriptor not found, no unit errors recorded", nodeName)
	}
}

func collectTaskErrors(task *distributedtask.Task) string {
	var errs []string
	if task.Error != "" {
		errs = append(errs, task.Error)
	}
	for id, u := range task.Units {
		if u.Error != "" {
			errs = append(errs, fmt.Sprintf("unit %s: %s", id, u.Error))
		}
	}
	for node, ack := range task.PostCompletionAcks {
		if !ack.Success && ack.Error != "" {
			errs = append(errs, fmt.Sprintf("ack %s: %s", node, ack.Error))
		}
	}
	if len(errs) == 0 {
		return "backup failed"
	}
	return strings.Join(errs, "; ")
}

// isRetryableUploadError returns true for transport-level object-store failures
// that the DTM scheduler may retry. Anything unclassified is permanent, matching
// the 2PC path. Backend SDKs wrap errors, so checks use errors.Is/errors.As.
func isRetryableUploadError(err error) bool {
	// Cancellation is the operator's decision, not a transient backend fault.
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	// The flow's own context carries no deadline, so a deadline reported here
	// came from the backend client's own timeout.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// --- task handle ---

type backupTaskHandle struct {
	mu       sync.Mutex
	stopCh   chan struct{}
	doneCh   chan struct{}
	cancelFn context.CancelFunc
}

func newBackupTaskHandle(_ string) *backupTaskHandle {
	return &backupTaskHandle{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func (h *backupTaskHandle) Terminate() {
	h.mu.Lock()
	defer h.mu.Unlock()
	select {
	case <-h.stopCh:
	default:
		close(h.stopCh)
	}
	if h.cancelFn != nil {
		h.cancelFn()
	}
}

func (h *backupTaskHandle) Done() <-chan struct{} {
	return h.doneCh
}

// setCancelFunc registers the flow's cancel. A Terminate that arrived before
// this point is applied here, so a cancel racing StartTask is never lost.
func (h *backupTaskHandle) setCancelFunc(fn context.CancelFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cancelFn = fn
	select {
	case <-h.stopCh:
		fn()
	default:
	}
}

func (h *backupTaskHandle) markDone() {
	close(h.doneCh)
}

type idleTaskHandle struct {
	doneCh chan struct{}
}

func newIdleTaskHandle() *idleTaskHandle {
	return &idleTaskHandle{doneCh: make(chan struct{})}
}

func (h *idleTaskHandle) Terminate() {
	select {
	case <-h.doneCh:
	default:
		close(h.doneCh)
	}
}

func (h *idleTaskHandle) Done() <-chan struct{} {
	return h.doneCh
}

type noopStatusPublisher struct{}

func (n *noopStatusPublisher) set(_ backup.Status) {}
func (n *noopStatusPublisher) setFailed(_ string)  {}

// --- DTM status mapping ---

func dtmStatusToBackup(task *distributedtask.Task) (backup.Status, string) {
	switch task.Status {
	case distributedtask.TaskStatusStarted:
		for _, u := range task.Units {
			if u.Status == distributedtask.UnitStatusInProgress {
				return backup.Transferring, ""
			}
		}
		return backup.Started, ""
	case distributedtask.TaskStatusSwapping:
		return backup.Transferred, ""
	case distributedtask.TaskStatusFinished:
		return backup.Success, ""
	case distributedtask.TaskStatusFailed:
		return backup.Failed, collectTaskErrors(task)
	case distributedtask.TaskStatusCancelled:
		return backup.Cancelled, "backup canceled"
	default:
		return backup.Started, ""
	}
}

func (p *BackupTaskProvider) dtmTaskToStatus(ctx context.Context, task *distributedtask.Task) *Status {
	payload, err := unmarshalTaskPayload(task.Payload)
	if err != nil {
		return &Status{Status: backup.Failed, Err: err.Error()}
	}

	st := &Status{
		StartedAt: task.StartedAt,
		Path:      "",
	}

	if task.Status.IsTerminal() {
		store, storeErr := coordBackend(p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
		if storeErr == nil {
			st.Path = store.HomeDir(payload.Bucket, payload.Path)
			meta, metaErr := store.Meta(ctx, GlobalBackupFile, payload.Bucket, payload.Path)
			if metaErr == nil {
				st.Status = meta.Status
				st.Err = meta.Error
				st.CompletedAt = meta.CompletedAt
				st.Size = float64(meta.PreCompressionSizeBytes) / (1024 * 1024 * 1024)
				st.BaseBackupID = meta.BaseBackupID
				return st
			}
		}
		st.Status, st.Err = dtmStatusToBackup(task)
		st.BaseBackupID = payload.BaseBackupID
		return st
	}

	store, storeErr := coordBackend(p.backends, payload.Backend, payload.ID, payload.Bucket, payload.Path)
	if storeErr == nil {
		st.Path = store.HomeDir(payload.Bucket, payload.Path)
	}
	st.Status, st.Err = dtmStatusToBackup(task)
	st.BaseBackupID = payload.BaseBackupID
	return st
}
