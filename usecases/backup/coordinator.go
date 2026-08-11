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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Op is the kind of a backup operation
type Op string

const (
	OpCreate  Op = "create"
	OpRestore Op = "restore"
)

var (
	errCannotCommit = errors.New("cannot commit")
	errMetaNotFound = errors.New("metadata not found")
	errUnknownOp    = errors.New("unknown backup operation")
	errCancelled    = errors.New("operation cancelled by user")
)

const (
	_BookingPeriod      = time.Second * 20
	_TimeoutNodeDown    = 7 * time.Minute
	_TimeoutQueryStatus = 5 * time.Second
	_TimeoutCanCommit   = 8 * time.Second
	_NextRoundPeriod    = 10 * time.Second
	_MaxNumberConns     = 16
)

type nodeMap map[string]*backup.NodeDescriptor

// participantStatus tracks status of a participant in a DBRO
type participantStatus struct {
	Status   backup.Status
	LastTime time.Time
	Reason   string
}

// Selector is used to select participant nodes
type Selector interface {
	// Shards gets all nodes on which this class is sharded
	Shards(ctx context.Context, class string) ([]string, error)
	// ListClasses returns a list of all existing classes
	// This will be needed if user doesn't include any classes
	ListClasses(ctx context.Context) []string

	// Backupable returns whether all given class can be backed up.
	Backupable(_ context.Context, classes []string) error
}

// UserLister resolves includeUsers selectors. ListAllUsers returns qualified
// "namespace:userId" ids (apikey.MakeUserKey) — the form selectors match
// against. Nil when dynamic DB users are disabled.
type UserLister interface {
	ListAllUsers() []string
}

// RoleLister resolves includeRoles selectors. ListAllRoles returns every role
// name, custom and built-in, which is the list selectors match against. Nil when
// RBAC is disabled.
type RoleLister interface {
	ListAllRoles() ([]string, error)
}

// coordinator coordinates a distributed backup and restore operation (DBRO):
//
// - It determines what request to send to which shard.
//
// - I will return an error, If any shards refuses to participate in DBRO.
//
// - It keeps all metadata needed to resume a DBRO in an external storage (e.g. s3).
//
// - When it starts it will check for any broken DBROs using its metadata.
//
// - It can resume a broken a DBRO
//
// - It marks the whole DBRO as failed if any shard fails to do its BRO.
//
// - The coordinator will try to repair previous DBROs whenever it is possible
type coordinator struct {
	// dependencies
	selector     Selector
	client       client
	schema       schemaManger
	log          logrus.FieldLogger
	nodeResolver NodeResolver
	backends     BackupBackendProvider

	// state
	shardSyncChan

	// timeouts
	timeoutNodeDown    time.Duration
	timeoutQueryStatus time.Duration
	timeoutCanCommit   time.Duration
	timeoutNextRound   time.Duration
}

// newcoordinator creates an instance which coordinates distributed BRO operations among many shards.
func newCoordinator(
	selector Selector,
	client client,
	schema schemaManger,
	log logrus.FieldLogger,
	nodeResolver NodeResolver,
	backends BackupBackendProvider,
) *coordinator {
	c := &coordinator{
		selector:           selector,
		client:             client,
		schema:             schema,
		log:                log,
		nodeResolver:       nodeResolver,
		backends:           backends,
		timeoutNodeDown:    _TimeoutNodeDown,
		timeoutQueryStatus: _TimeoutQueryStatus,
		timeoutCanCommit:   _TimeoutCanCommit,
		timeoutNextRound:   _NextRoundPeriod,
	}
	c.setSlotLogger(log)
	return c
}

// operation holds the descriptor and per-participant status for a single
// backup or restore call. It belongs to the call, not the coordinator: a
// cancelled operation's goroutine keeps running after its slot is handed to
// the next one (see [slotOwner]), and coordinator-owned state would let the
// two race on the same map.
type operation struct {
	descriptor   *backup.DistributedBackupDescriptor
	participants map[string]participantStatus
}

func newOperation(desc *backup.DistributedBackupDescriptor) *operation {
	return &operation{descriptor: desc, participants: make(map[string]participantStatus, 16)}
}

// publishStatus mirrors the descriptor's outcome onto the slot. Reports
// whether the slot took it; false means it moved to another operation or
// holds a status this outcome may not overwrite (see [backupStat.canAdvanceTo]).
func (op *operation) publishStatus(slot slotOwner) bool {
	if op.descriptor.Status == backup.Failed {
		return slot.setFailed(op.descriptor.Error)
	}
	return slot.set(op.descriptor.Status)
}

func (c *coordinator) Nodes(ctx context.Context, req *Request) (map[string]string, error) {
	leader := c.nodeResolver.LeaderID()
	if leader == "" {
		return nil, fmt.Errorf("backup Op %s: %w, try again later", req.Method, types.ErrLeaderNotFound)
	}
	groups, err := c.groupByShard(ctx, req.Classes, leader)
	if err != nil {
		return nil, err
	}

	res := map[string]string{}

	for k := range groups {
		host, found := c.nodeResolver.NodeHostname(k)
		if !found {
			return nil, fmt.Errorf("cannot resolve hostname for %q", k)
		}
		res[k] = host
	}

	return res, nil
}

// Backup coordinates a distributed backup among participants
func (c *coordinator) Backup(ctx context.Context, cstore coordStore, req *Request) (err error) {
	req.Method = OpCreate
	leader := c.nodeResolver.LeaderID()
	if leader == "" {
		return fmt.Errorf("backup Op %s: %w, try again later", req.Method, types.ErrLeaderNotFound)
	}
	groups, err := c.groupByShard(ctx, req.Classes, leader)
	if err != nil {
		return err
	}
	// make sure there is no active backup
	prevID, slot := c.lastOp.renew(req.ID, cstore.HomeDir(req.Bucket, req.Path), req.Bucket, req.Path)
	if prevID != "" {
		return backup.NewErrUnprocessable(fmt.Errorf("backup %s already in progress", prevID))
	}
	// From here the slot is ours until the goroutine below takes over, so
	// every error return must release it or it blocks later backups until
	// restart. Restores use their own coordinator and slot, unaffected.
	defer func() {
		if err != nil {
			slot.release()
		}
	}()

	compressionType, err := CompressionTypeFromLevel(req.Level)
	if err != nil {
		return backup.NewErrUnprocessable(err)
	}

	op := newOperation(&backup.DistributedBackupDescriptor{
		StartedAt:       time.Now().UTC(),
		Status:          backup.Started,
		ID:              req.ID,
		Nodes:           groups,
		Version:         Version,
		ServerVersion:   config.ServerVersion,
		Leader:          leader,
		CompressionType: compressionType,
		BaseBackupID:    req.BaseBackupID,
		Users:           req.Users,
		Roles:           req.Roles,
	})

	nodes, err := c.canCommit(ctx, op, req)
	if err != nil {
		return err
	}

	overrideBucket := req.Bucket
	overridePath := req.Path
	if putErr := cstore.PutMeta(ctx, GlobalBackupFile, op.descriptor, overrideBucket, overridePath); putErr != nil {
		return fmt.Errorf("coordinator: cannot init meta file: %w", putErr)
	}

	statusReq := StatusRequest{
		Method:       OpCreate,
		ID:           req.ID,
		Backend:      req.Backend,
		Bucket:       req.Bucket,
		Path:         req.Path,
		BaseBackupID: req.BaseBackupID,
	}

	f := func() {
		defer slot.release()
		ctx := context.Background()
		c.commit(ctx, op, &statusReq, nodes, false, slot)
		logFields := logrus.Fields{"action": OpCreate, "backup_id": req.ID}
		if err := cstore.PutMeta(ctx, GlobalBackupFile, op.descriptor, overrideBucket, overridePath); err != nil {
			c.log.WithFields(logFields).Errorf("coordinator: put_meta: %v", err)
		}
		if op.descriptor.Status == backup.Success {
			c.log.WithFields(logFields).Info("coordinator: backup completed successfully")
		} else {
			c.log.WithFields(logFields).Errorf("coordinator: %s", op.descriptor.Error)
		}
	}
	enterrors.GoWrapper(f, c.log)

	return nil
}

// Restore coordinates a distributed restoration among participants
func (c *coordinator) Restore(
	ctx context.Context,
	store coordStore,
	req *Request,
	desc *backup.DistributedBackupDescriptor,
	schema []backup.ClassDescriptor,
) (err error) {
	req.Method = OpRestore

	// Check if a cancellation is already in progress before asking nodes to commit.
	if existingMeta, err := store.Meta(ctx, GlobalRestoreFile, req.Bucket, req.Path); err == nil {
		if existingMeta.Status == backup.Cancelling {
			// Free the slot only if it still holds this cancelled restore; a
			// retry under the same id may already own it.
			released, held := c.lastOp.resetIfCancelled(desc.ID)
			c.log.WithFields(logrus.Fields{
				"action":      OpRestore,
				"backup_id":   desc.ID,
				"slot_freed":  released,
				"slot_holder": held.ID,
				"slot_status": held.Status,
			}).Info("restore cancellation already in progress, nothing started")
			// Returning nil would surface as STARTED. A cancel whose final write
			// failed leaves this stuck on CANCELLING forever, so tell the caller
			// to repeat the cancel rather than just wait.
			return backup.NewErrUnprocessable(fmt.Errorf(
				"restore %s cancellation in progress, please wait for it to complete; "+
					"if it does not, repeat the cancel to clear it", desc.ID))
		}
	}

	// make sure there is no active backup
	prevID, slot := c.lastOp.renew(desc.ID, store.HomeDir(req.Bucket, req.Path), req.Bucket, req.Path)
	if prevID != "" {
		return backup.NewErrUnprocessable(fmt.Errorf("restoration %s already in progress", prevID))
	}
	// The slot is ours until the goroutine below takes over, so every error
	// return must release it or it blocks later restores until restart.
	// release(), not reset(): a cancel may already hold it.
	defer func() {
		if err != nil {
			slot.release()
		}
	}()

	op := newOperation(desc.ResetStatus())

	// Time canCommit phase (initiates file staging on all nodes)
	canCommitStart := time.Now()
	nodes, err := c.canCommit(ctx, op, req)
	c.observeRestorePhase("prepare", time.Since(canCommitStart))
	if err != nil {
		return err
	}

	// Set status to Transferring now that staging has begun. A refusal is
	// nothing to act on: it says a cancel of this restore is already stamped,
	// which the goroutine below checks for on its own.
	op.descriptor.Status = backup.Transferring
	slot.set(backup.Transferring)

	overrideBucket := req.Bucket
	overridePath := req.Path

	// initial put so restore status is immediately available
	if putErr := store.PutMeta(ctx, GlobalRestoreFile, op.descriptor, overrideBucket, overridePath); putErr != nil {
		abortReq := &AbortRequest{Method: OpRestore, ID: desc.ID, Backend: req.Backend}
		c.abortAll(ctx, abortReq, nodes)
		return fmt.Errorf("put initial metadata: %w", putErr)
	}

	statusReq := StatusRequest{Method: OpRestore, ID: desc.ID, Backend: req.Backend, Bucket: overrideBucket, Path: overridePath}
	g := func() {
		defer slot.release()
		ctx := context.Background()

		// checkStorageCancelled reads from storage to check if restore was cancelled.
		// Storage is the authoritative source since it works across all nodes in the cluster.
		// Returns true if the restore should stop due to cancellation.
		// Treats both CANCELLING and CANCELLED as cancellation signals - CANCELLING means
		// another coordinator has claimed and is processing the cancellation.
		checkStorageCancelled := func() bool {
			storedMeta, err := store.Meta(ctx, GlobalRestoreFile, overrideBucket, overridePath)
			if err != nil {
				return false // Can't read storage, continue with operation
			}
			if storedMeta.Status.IsCancellation() {
				op.descriptor.Status = backup.Cancelled
				op.descriptor.Error = storedMeta.Error
				if op.descriptor.Error == "" {
					op.descriptor.Error = errCancelled.Error()
				}
				// No slot write here: the caller returns true, and the deferred
				// release clears the slot right after, so nothing would read it.
				return true
			}
			return false
		}

		// Time commit polling phase (waits for all nodes to finish staging)
		commitStart := time.Now()
		c.commit(ctx, op, &statusReq, nodes, true, slot)
		c.observeRestorePhase("object_storage_download", time.Since(commitStart))

		// Losing the slot means another restore has since claimed it; writing
		// below would report that restore as finished/replay this one over it.
		// The cancel already wrote CANCELLED, so nothing is lost by stopping.
		if !slot.holds() {
			c.log.WithFields(logrus.Fields{
				"action":       OpRestore,
				"backup_id":    desc.ID,
				"final_status": op.descriptor.Status,
			}).Info("restore no longer holds the slot, stopping without publishing")
			return
		}

		// Check storage for cancellation before transitioning to Finalizing.
		// This handles the case where CancelRestore was called (possibly on a different node)
		// and wrote CANCELLED status to storage while we were in commit phase.
		if checkStorageCancelled() {
			c.log.WithField("backup_id", desc.ID).Info("restore cancelled (detected from storage after commit)")
			// Don't write to storage - CancelRestore already wrote the CANCELLED status
			return
		}

		// Block cancellation by setting status to Finalizing before schema apply.
		// Only proceed if staging was successful (Transferred = staging complete).
		// The slot's set() is the check-and-set: it refuses a write that would
		// walk back a cancellation (in flight or already landed), closing the
		// race a separate read-then-write would leave open. A schema apply over
		// RAFT can't be undone once started; see [backupStat.canAdvanceTo].
		if op.descriptor.Status == backup.Transferred {
			if !slot.set(backup.Finalizing) {
				op.descriptor.Status = backup.Cancelled
				op.descriptor.Error = errCancelled.Error()
			} else {
				op.descriptor.Status = backup.Finalizing
				if err := store.PutMeta(ctx, GlobalRestoreFile, op.descriptor, overrideBucket, overridePath); err != nil {
					c.log.WithField("backup_id", desc.ID).Errorf("failed to persist finalizing status: %v", err)
				}
			}
		}

		// Only proceed with schema apply if we successfully transitioned to Finalizing
		// Skip if status is Cancelled, Failed, or any other non-Finalizing state
		if op.descriptor.Status == backup.Finalizing {
			// Time schema apply phase (Raft commits for each class)
			schemaApplyStart := time.Now()
			c.restoreClasses(ctx, op, schema, req)
			c.observeRestorePhase("schema_apply", time.Since(schemaApplyStart))

			// Set final status - restoreClasses may have set Failed, otherwise set Success
			if op.descriptor.Status == backup.Finalizing {
				op.descriptor.Status = backup.Success
			}
		}
		// A refused publish means the slot moved on, or already holds this exact
		// outcome; only the former is a reason to stop. A cancel cannot land
		// during schema apply — the slot reads Finalizing there, which refuses
		// cancellations — so that path always publishes.
		published := op.publishStatus(slot)
		restoreIsItselfCancelled := op.descriptor.Status.IsCancellation() && slot.holds()
		if !published && !restoreIsItselfCancelled {
			c.log.WithFields(logrus.Fields{
				"action":       OpRestore,
				"backup_id":    desc.ID,
				"final_status": op.descriptor.Status,
			}).Info("restore outcome refused by the slot, stopping without publishing")
			return
		}

		logFields := logrus.Fields{"action": OpRestore, "backup_id": desc.ID}
		if err := store.PutMeta(ctx, GlobalRestoreFile, op.descriptor, overrideBucket, overridePath); err != nil {
			c.log.WithFields(logFields).Errorf("coordinator: put_meta: %v", err)
		}
		if op.descriptor.Status == backup.Success {
			c.log.WithFields(logFields).Info("coordinator: backup restored successfully")
		} else {
			c.log.WithFields(logFields).Errorf("coordinator: %v", op.descriptor.Error)
		}
	}
	enterrors.GoWrapper(g, c.log)

	return nil
}

// observeRestorePhase records the duration of a restore phase to Prometheus
func (c *coordinator) observeRestorePhase(phase string, duration time.Duration) {
	metric, err := monitoring.GetMetrics().RestorePhaseDurations.GetMetricWithLabelValues(phase)
	if err == nil {
		metric.Observe(duration.Seconds())
	}
}

// restoreClasses attempts to restore all classes.
// It continues attempting to restore other classes even if some restoration attempts fail.
// The failure of one class restoration does not necessarily indicate failure for all classes;
// other classes might be successfully restored.

func (c *coordinator) restoreClasses(
	ctx context.Context,
	op *operation,
	schema []backup.ClassDescriptor,
	req *Request,
) {
	// Only proceed if status is Finalizing (set by caller before schema apply)
	if op.descriptor.Status != backup.Finalizing {
		c.log.WithFields(logrus.Fields{
			"action":          "restore_classes",
			"backup_id":       op.descriptor.ID,
			"expected_status": backup.Finalizing,
			"actual_status":   op.descriptor.Status,
		}).Error("unexpected status before schema apply")
		op.descriptor.Error = fmt.Sprintf("unexpected status %q before schema apply, expected %q", op.descriptor.Status, backup.Finalizing)
		op.descriptor.Status = backup.Failed
		return
	}
	restoreErrors := make([]string, 0, 5)
	hasReqClasses := len(req.Classes) > 0
	for _, cls := range schema {
		// Check for context cancellation between class restores
		// Note: Once in Finalizing state, external cancellation via CancelRestore() is blocked,
		// but we still respect context cancellation for internal consistency
		if err := ctx.Err(); err != nil {
			c.log.WithFields(logrus.Fields{
				"action":    "restore_classes",
				"backup_id": op.descriptor.ID,
				"class":     cls.Name,
			}).Warn("schema apply interrupted by context cancellation")
			op.descriptor.Error = fmt.Sprintf("schema apply interrupted: %v", err)
			op.descriptor.Status = backup.Failed
			return
		}

		if hasReqClasses && !slices.Contains(req.Classes, cls.Name) {
			continue
		}
		if err := c.schema.RestoreClass(ctx, &cls, req.NodeMapping, req.RestoreOverwriteAlias, !c.schema.NamespacesEnabled()); err != nil {
			op.descriptor.Error = fmt.Sprintf("restore class %q: %v", cls.Name, err)
			restoreErrors = append(restoreErrors, fmt.Sprintf("%q: %v", cls.Name, err))
		}
	}
	if len(restoreErrors) > 0 {
		op.descriptor.Status = backup.Failed
		op.descriptor.Error = fmt.Sprintf("could not restore classes: %v", restoreErrors)
	}
}

func (c *coordinator) OnStatus(ctx context.Context, store coordStore, req *StatusRequest) (*Status, error) {
	// check if backup is still active
	st := c.lastOp.get()
	if st.ID == req.ID {
		return &Status{Path: st.Path, StartedAt: st.Starttime, Status: st.Status, Err: st.Err}, nil
	}
	filename := GlobalBackupFile
	if req.Method == OpRestore {
		filename = GlobalRestoreFile
	}

	// The backup might have been already created.
	meta, err := store.Meta(ctx, filename, store.bucket, store.path)
	if err != nil {
		path := st.Path
		if errors.As(err, &backup.ErrNotFound{}) {
			return nil, fmt.Errorf("coordinator cannot get status: %w: %q: %w store: %v", errMetaNotFound, path, err, st)
		}
		return nil, fmt.Errorf("coordinator cannot get status: %q: %w store: %v", path, err, st)
	}

	status := &Status{
		Path:         store.HomeDir(store.bucket, store.path),
		StartedAt:    meta.StartedAt,
		CompletedAt:  meta.CompletedAt,
		Status:       meta.Status,
		Err:          meta.Error,
		Size:         float64(meta.PreCompressionSizeBytes) / (1024 * 1024 * 1024), // Convert bytes to GiB,
		BaseBackupID: meta.BaseBackupID,
	}
	if reason, ok := c.lastOp.rememberedFailure(req.ID); ok && !isFinalStatus(meta.Status) {
		// The operation ended failed and writing that outcome to the backend
		// is what failed, so the descriptor still reads as in progress. Serving
		// it would report a failed backup as running for as long as this node
		// is up.
		status.Status = backup.Failed
		status.Err = reason
	}
	return status, nil
}

// isFinalStatus reports whether a stored descriptor status is the operation's
// last word, and so must not be second-guessed from memory.
func isFinalStatus(st backup.Status) bool {
	return st == backup.Success || st == backup.Failed || st == backup.Cancelled
}

// canCommitErrFromResponse promotes a refused [CanCommitResponse] into a
// typed error. When the response has [CanCommitErrInFlightReindex] kind, we
// wrap the shared [backup.ErrBackupBlockedByInFlightReindex] sentinel so
// upstream `errors.Is` checks succeed across the RPC boundary. Empty or
// [CanCommitErrCannotCommit] kinds (including responses from older nodes
// that don't set the field) keep the legacy [errCannotCommit] wrapping so
// existing callers and tests continue to match.
func canCommitErrFromResponse(resp *CanCommitResponse) error {
	if resp == nil {
		return errCannotCommit
	}
	switch resp.ErrKind {
	case CanCommitErrInFlightReindex:
		return fmt.Errorf("%w: %s", backup.ErrBackupBlockedByInFlightReindex, resp.Err)
	default:
		return fmt.Errorf("%w : %v", errCannotCommit, resp.Err)
	}
}

// canCommit asks candidates if they agree to participate in DBRO
// It returns and error if any candidates refuses to participate
func (c *coordinator) canCommit(ctx context.Context, op *operation, req *Request) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutCanCommit)
	defer cancel()

	// Apply node mapping to the descriptor shall happen before
	// asking candidates if they agree to participate in DBRO and before creating the request channel.
	// This ensures that the request channel contains the correct node names and RESOLVES
	// correctly the NEW node names and hosts if mapping exists.
	// NOTE: This could be leveraged for adjusting number of nodes in the schema (as future implementation).
	op.descriptor.ApplyNodeMapping()

	reqChan := make(chan *Request)
	g, ctx := enterrors.NewErrorGroupWithContextWrapper(c.log, ctx)
	g.SetLimit(_MaxNumberConns)
	g.Go(func() error {
		defer close(reqChan)
		for nodeName, gr := range op.descriptor.Nodes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// If we have a nodeMapping with the node name from the backup, replace the node with the new one
			nodeName = op.descriptor.ToMappedNodeName(nodeName)

			host, found := c.nodeResolver.NodeHostname(nodeName)
			if !found {
				return fmt.Errorf("cannot resolve hostname for %q, nodes=%v, nodeMapping=%v", nodeName, op.descriptor.Nodes, op.descriptor.NodeMapping)
			}

			reqChan <- &Request{
				NodeName:          nodeName,
				NodeHost:          host,
				Method:            req.Method,
				ID:                op.descriptor.ID,
				Backend:           req.Backend,
				Classes:           gr.Classes,
				Users:             req.Users,
				Roles:             req.Roles,
				Duration:          _BookingPeriod,
				NodeMapping:       op.descriptor.NodeMapping,
				Compression:       req.Compression,
				Bucket:            req.Bucket,
				Path:              req.Path,
				UserRestoreOption: req.UserRestoreOption,
				RbacRestoreOption: req.RbacRestoreOption,
				BaseBackupID:      op.descriptor.BaseBackupID,
			}
		}
		return nil
	})

	mutex := sync.RWMutex{}
	nodes := make(map[string]string, len(op.descriptor.Nodes))
	for req := range reqChan {
		g.Go(func() error {
			resp, err := c.client.CanCommit(ctx, req.NodeHost, req)
			if err == nil && resp.Timeout == 0 {
				err = canCommitErrFromResponse(resp)
			}
			if err != nil {
				return fmt.Errorf("node %q: %w", req.NodeName, err)
			}
			mutex.Lock()
			nodes[req.NodeName] = req.NodeHost
			mutex.Unlock()
			return nil
		})
	}
	abortReq := &AbortRequest{Method: req.Method, ID: op.descriptor.ID, Backend: req.Backend}
	if err := g.Wait(); err != nil {
		c.abortAll(ctx, abortReq, nodes)
		return nil, err
	}
	return nodes, nil
}

// commit tells each participant to commit its backup operation
// It stores the final result in the provided backend
func (c *coordinator) commit(ctx context.Context,
	op *operation,
	req *StatusRequest,
	node2Addr map[string]string,
	toleratePartialFailure bool,
	slot slotOwner,
) {
	// create a new copy for commitAll and queryAll to mutate
	node2Host := make(map[string]string, len(node2Addr))
	for k, v := range node2Addr {
		node2Host[k] = v
	}

	// Check for external cancellation before starting
	if cancelledExternally(slot) {
		c.log.WithField("backup_id", req.ID).Info("commit aborted: operation was cancelled externally")
		op.descriptor.Status = backup.Cancelled
		op.descriptor.Error = errCancelled.Error()
		return
	}

	nFailures := c.commitAll(ctx, op, req, node2Host)
	retryAfter := c.timeoutNextRound / 5 // 2s for first time
	canContinue := len(node2Host) > 0 && (toleratePartialFailure || nFailures == 0)
	for canContinue {
		// Check for external cancellation in polling loop
		if cancelledExternally(slot) {
			c.log.WithField("backup_id", req.ID).Info("commit polling aborted: operation was cancelled externally")
			// Mark remaining nodes as cancelled
			for node := range node2Host {
				st := op.participants[node]
				st.Status = backup.Cancelled
				st.Reason = errCancelled.Error()
				op.participants[node] = st
			}
			op.descriptor.Status = backup.Cancelled
			op.descriptor.Error = errCancelled.Error()
			return
		}

		select {
		case <-time.After(retryAfter):
			// continue with polling
		case <-ctx.Done():
			c.log.WithField("backup_id", req.ID).Info("commit polling aborted: context cancelled")
			op.descriptor.Status = backup.Cancelled
			op.descriptor.Error = "restore cancelled: context cancelled"
			return
		}
		retryAfter = c.timeoutNextRound
		nFailures += c.queryAll(ctx, op, req, node2Host)
		canContinue = len(node2Host) > 0 && (toleratePartialFailure || nFailures == 0)
	}
	if !toleratePartialFailure && nFailures > 0 {
		req := &AbortRequest{Method: req.Method, ID: req.ID, Backend: req.Backend}
		c.abortAll(context.Background(), req, node2Addr)
	}
	op.descriptor.CompletedAt = time.Now().UTC()
	// For restore operations, successful staging means "Transferred" (ready for schema apply)
	// For backup operations, successful staging means "Success" (operation complete)
	status := backup.Success
	if req.Method == OpRestore {
		status = backup.Transferred
	}
	reason := ""
	groups := op.descriptor.Nodes
	var totalPreCompressionSize int64

	// Read backup descriptors from each node to aggregate pre-compression sizes
	for node, p := range op.participants {
		st := groups[op.descriptor.ToOriginalNodeName(node)]
		st.Status, st.Error = p.Status, p.Reason
		if p.Status != backup.Success {
			if p.Status == backup.Cancelled {
				status = backup.Cancelled
				st.Status = backup.Cancelled
				if reason == "" {
					reason = p.Reason
				}
			} else {
				status = backup.Failed
				reason = p.Reason

				if p.Reason == errCancelled.Error() || strings.Contains(p.Reason, context.Canceled.Error()) {
					status = backup.Cancelled
					st.Status = backup.Cancelled
				}
			}
		} else {
			// Try to read the node's backup descriptor to get pre-compression size
			// for the whole cluster (not just the node)
			// Skip this for restore operations
			if req.Method != OpRestore {
				if backend, err := c.backends.BackupBackend(req.Backend, modulecapabilities.BackendUseCaseBackup); err == nil {
					// Create a nodeStore for this specific node
					nodeBackupID := fmt.Sprintf("%s/%s", req.ID, node)
					nodeStore := nodeStore{
						objectStore: objectStore{
							backend:  backend,
							backupId: nodeBackupID,
							bucket:   req.Bucket,
							path:     req.Path,
							node:     node,
						},
					}

					if meta, err := nodeStore.Meta(ctx, req.ID, req.Bucket, req.Path); err == nil {
						st.PreCompressionSizeBytes = meta.PreCompressionSizeBytes
						totalPreCompressionSize += meta.PreCompressionSizeBytes
						c.log.WithFields(logrus.Fields{
							"node":                    node,
							"preCompressionSizeBytes": meta.PreCompressionSizeBytes,
							"totalPreCompressionSize": totalPreCompressionSize,
						}).Debug("read node backup descriptor pre-compression size")
					} else {
						c.log.WithFields(logrus.Fields{
							"node":  node,
							"error": err,
						}).Warn("could not read node backup descriptor for pre-compression size")
					}
				}
			}
		}
		groups[node] = st
	}
	op.descriptor.Status = status
	// Respect external cancellation from CancelRestore() - if the slot was
	// already stamped, propagate that to descriptor so storage writes are consistent
	if cancelledExternally(slot) {
		op.descriptor.Status = backup.Cancelled
		if reason == "" {
			reason = "restore canceled by user"
		}
	}
	op.descriptor.Error = reason
	// Ignoring the refusal is deliberate: this publishes the staging outcome
	// for polls, and the caller that has something to decide (Restore) asks
	// again right after, through holds() and the Finalizing write.
	op.publishStatus(slot)
	op.descriptor.PreCompressionSizeBytes = totalPreCompressionSize
}

// cancelledExternally reports whether this operation's own slot has been
// cancelled, counting a cancel still in flight (Cancelling). A slot this
// operation no longer holds reads as not cancelled: its status belongs to
// whichever operation claimed it next.
func cancelledExternally(slot slotOwner) bool {
	st, ok := slot.status()
	return ok && st.IsCancellation()
}

// queryAll queries all participant and store their statuses internally
//
// It returns the number of failed node backups
func (c *coordinator) queryAll(ctx context.Context, op *operation, req *StatusRequest, nodes map[string]string) int {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutQueryStatus)
	defer cancel()

	c.log.WithFields(logrus.Fields{
		"action":   "coordinator_query_all",
		"duration": c.timeoutQueryStatus,
		"nodes":    nodes,
		"method":   req.Method,
		"backend":  req.Backend,
	}).Debug("context.WithTimeout")

	rs := make([]partialStatus, len(nodes))
	g, ctx := enterrors.NewErrorGroupWithContextWrapper(c.log, ctx)
	g.SetLimit(_MaxNumberConns)
	i := 0
	for node, hostname := range nodes {
		j := i
		hostname := hostname
		rs[j].node = node
		g.Go(func() error {
			rs[j].StatusResponse, rs[j].err = c.client.Status(ctx, hostname, req)
			return nil
		})
		i++
	}
	g.Wait()
	n, now := 0, time.Now()
	for _, r := range rs {
		st := op.participants[r.node]
		if r.err == nil {
			st.LastTime, st.Status, st.Reason = now, r.Status, r.Err
			if r.Status == backup.Success {
				delete(nodes, r.node)
			}
			if r.Status == backup.Failed || r.Status == backup.Cancelled {
				delete(nodes, r.node)
				n++
			}
		} else if now.Sub(st.LastTime) > c.timeoutNodeDown {
			n++
			st.Status = backup.Failed
			st.Reason = fmt.Sprintf("node %q might be down: %v", r.node, r.err.Error())
			if strings.Contains(st.Reason, context.Canceled.Error()) {
				st.Status = backup.Cancelled
			}
			delete(nodes, r.node)
		}
		op.participants[r.node] = st
	}
	return n
}

// commitAll tells all participants to proceed with their backup operations
// It returns the number of failures
func (c *coordinator) commitAll(ctx context.Context, op *operation, req *StatusRequest, nodes map[string]string) int {
	type pair struct {
		node string
		err  error
	}
	// Buffer one slot per node so a failing worker never blocks on the send.
	// The consumer only runs after the submit loop finishes, so an unbuffered
	// channel would let the first _MaxNumberConns failures hold every g.Go slot
	// while blocked on the send, deadlocking the submit loop.
	errChan := make(chan pair, len(nodes))
	aCounter := int64(len(nodes))
	g, ctx := enterrors.NewErrorGroupWithContextWrapper(c.log, ctx)
	g.SetLimit(_MaxNumberConns)
	for node, hostname := range nodes {
		node, hostname := node, hostname
		g.Go(func() error {
			defer func() {
				if atomic.AddInt64(&aCounter, -1) == 0 {
					close(errChan)
				}
			}()
			err := c.client.Commit(ctx, hostname, req)
			if err != nil {
				errChan <- pair{node, err}
			}
			return nil
		})
	}
	nFailures := 0
	for x := range errChan {
		st := op.participants[x.node]
		st.Reason = "might be down:" + x.err.Error()
		if strings.Contains(x.err.Error(), context.Canceled.Error()) {
			st.Status = backup.Cancelled
		} else {
			st.Status = backup.Failed
		}
		op.participants[x.node] = st
		c.log.WithField("action", req.Method).
			WithField("backup_id", req.ID).
			WithField("node", x.node).Error(x.err)
		delete(nodes, x.node)
		nFailures++
		continue
	}
	return nFailures
}

// abortAll tells every node to abort transaction
func (c *coordinator) abortAll(ctx context.Context, req *AbortRequest, nodes map[string]string) {
	for name, hostname := range nodes {
		if err := c.client.Abort(ctx, hostname, req); err != nil {
			c.log.WithField("action", req.Method).
				WithField("backup_id", req.ID).
				WithField("node", name).Errorf("abort %v", err)
		}
	}
}

// groupByShard returns classes group by nodes
func (c *coordinator) groupByShard(ctx context.Context, classes []string, leader string) (nodeMap, error) {
	nodes := c.nodeResolver.AllNames()
	m := make(nodeMap, len(nodes))

	// start by collecting nodes which have content
	for _, cls := range classes {
		nodes, err := c.selector.Shards(ctx, cls)
		if err != nil {
			continue
		}
		for _, node := range nodes {
			if node == leader {
				continue
			}
			nd, ok := m[node]
			if !ok {
				nd = &backup.NodeDescriptor{Classes: make([]string, 0, 5)}
			}
			nd.Classes = append(nd.Classes, cls)
			m[node] = nd
		}
	}

	// leader ensure all backup all classes regardless if they have content or not
	m[leader] = &backup.NodeDescriptor{Classes: slices.Clone(classes)}
	return m, nil
}

// partialStatus tracks status of a single backup operation
type partialStatus struct {
	node string
	*StatusResponse
	err error
}

func CompressionTypeFromLevel(c CompressionLevel) (backup.CompressionType, error) {
	switch c {
	case GzipBestCompression, GzipBestSpeed, GzipDefaultCompression:
		return backup.CompressionGZIP, nil
	case ZstdBestCompression, ZstdBestSpeed, ZstdDefaultCompression:
		return backup.CompressionZSTD, nil
	case NoCompression:
		return backup.CompressionNone, nil
	default:
		return "", fmt.Errorf("invalid compression level: %v", c)
	}
}
