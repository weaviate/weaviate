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

package replica

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

const (
	defaultBackOffInitialInterval = time.Millisecond * 250
	defaultBackOffMaxElapsedTime  = time.Second * 128

	// defaultPullHostHedgeDelay is how long a read waits on one replica before also trying an idle one
	defaultPullHostHedgeDelay = 500 * time.Millisecond

	// defaultPrepareTimeout bounds the prepare phase, within the caller's deadline
	defaultPrepareTimeout = 20 * time.Second

	// defaultCommitTimeout bounds the commit phase, which must not be caller-cancellable
	defaultCommitTimeout = 2 * time.Minute
)

type (
	// readyOp asks a replica if it is ready to commit
	readyOp func(_ context.Context, host, requestID string) error

	// readyOp asks a replica to execute the actual operation
	commitOp[T any] func(_ context.Context, host, requestID string) (T, error)

	// readOp defines a generic read operation
	readOp[T any] func(_ context.Context, host string, fullRead bool) (T, error)

	// onResult defines a hook called when the coordinator reads a result from the commitCh
	onResult[T any] func(result Result[T], successes []T, failures []T) ([]T, []T, bool, error)

	// onFlatten defines a hook to flatten results into a single of outputs
	onFlatten[T, R any] func(batchSize int, results []T, defaultErr error) []R

	// coordinator coordinates replication of write and read requests
	coordinator[T, R any] struct {
		Client
		Router  types.Router
		metrics *Metrics
		log     logrus.FieldLogger
		Class   string
		Shard   string
		TxID    string // transaction ID
		// wait twice this duration for the first Pull backoff for each host
		pullBackOffPreInitialInterval time.Duration
		pullBackOffMaxElapsedTime     time.Duration // stop retrying after this long
		pullHostHedgeDelay            time.Duration
		deletionStrategy              string
	}
)

// NewWriteCoordinator used by the replicator to write objects to replicas
func NewWriteCoordinator[T, R any](client Client,
	router types.Router,
	metrics *Metrics,
	className, shard, requestID string,
	l logrus.FieldLogger,
) *coordinator[T, R] {
	return &coordinator[T, R]{
		Client:                        client,
		Router:                        router,
		metrics:                       metrics,
		log:                           l,
		Class:                         className,
		Shard:                         shard,
		TxID:                          requestID,
		pullBackOffPreInitialInterval: defaultBackOffInitialInterval / 2,
		pullBackOffMaxElapsedTime:     defaultBackOffMaxElapsedTime,
		pullHostHedgeDelay:            defaultPullHostHedgeDelay,
	}
}

// NewReadCoordinator used by the Finder to read objects from replicas
func NewReadCoordinator[T any](router types.Router,
	metrics *Metrics,
	className, shard, deletionStrategy string,
	log logrus.FieldLogger,
) *coordinator[T, any] {
	return &coordinator[T, any]{
		Router:                        router,
		Class:                         className,
		Shard:                         shard,
		log:                           log,
		metrics:                       metrics,
		pullBackOffPreInitialInterval: defaultBackOffInitialInterval / 2,
		pullBackOffMaxElapsedTime:     defaultBackOffMaxElapsedTime,
		pullHostHedgeDelay:            defaultPullHostHedgeDelay,
		deletionStrategy:              deletionStrategy,
	}
}

var _ Client = (*coordinator[any, any])(nil)

// broadcast sends write request to all replicas (first phase of a two-phase commit)
func (c *coordinator[T, R]) broadcast(ctx context.Context,
	replicas []string,
	op readyOp, level int,
) <-chan Result[string] {
	// prepare tells replicas to be ready
	prepare := func() <-chan Result[string] {
		resChan := make(chan Result[string], len(replicas))
		f := func() { // broadcast
			defer close(resChan)
			var wg sync.WaitGroup
			wg.Add(len(replicas))
			for _, replica := range replicas {
				replica := replica
				g := func() {
					defer wg.Done()
					err := op(ctx, replica, c.TxID)
					resChan <- Result[string]{replica, err}
				}
				enterrors.GoWrapper(g, c.log)
			}
			wg.Wait()
		}
		enterrors.GoWrapper(f, c.log)
		return resChan
	}

	// handle responses to prepare requests
	resChan := make(chan Result[string], len(replicas))
	required := level
	f := func() {
		defer close(resChan)
		actives := make([]Result[string], 0, level) // cache for active replicas
		var replicaErrs []error
		for r := range prepare() {
			if r.Err != nil { // connection error
				c.log.WithField("op", "broadcast").Warn(r.Err)
				// Attach the failing replica identifier so the resulting
				// error remains actionable regardless of whether the
				// per-replica op wrapped it with host context.
				replicaErrs = append(replicaErrs, annotateReplicaErr(r.Value, r.Err))
				continue
			}

			level--
			if level > 0 { // cache since level has not been reached yet
				actives = append(actives, r)
				continue
			}
			if level == 0 { // consistency level has been reached
				for _, x := range actives {
					resChan <- x
				}
			}
			resChan <- r
		}
		if level > 0 { // abort: nothing has been sent to the caller
			fs := logrus.Fields{"op": "broadcast", "active": len(actives), "total": len(replicas)}
			c.log.WithFields(fs).Error("abort")
			// abort even if the caller is gone: an unaborted prepare stays pending on the replica
			abortCtx := context.WithoutCancel(ctx)
			for _, node := range replicas {
				c.Abort(abortCtx, node, c.Class, c.Shard, c.TxID)
			}
			resChan <- Result[string]{Err: replicaerrors.NewNotEnoughReplicasErrorWithCounts(required, len(actives), errors.Join(replicaErrs...))}
		}
	}
	enterrors.GoWrapper(f, c.log)
	return resChan
}

// commitAll tells replicas to commit pending updates related to a specific request
// (second phase of a two-phase commit)
func (c *coordinator[T, R]) commitAll(ctx context.Context,
	broadcastCh <-chan Result[string],
	op commitOp[T],
	callback func(successful int),
) <-chan Result[T] {
	replyCh := make(chan Result[T], cap(broadcastCh))
	f := func() { // tells active replicas to commit
		// tells active replicas to commit

		var successful atomic.Int32

		defer func() {
			if callback != nil {
				callback(int(successful.Load()))
			}
		}()

		wg := sync.WaitGroup{}

		for res := range broadcastCh {
			if res.Err != nil {
				replyCh <- Result[T]{Err: res.Err}
				continue
			}
			replica := res.Value
			wg.Add(1)
			g := func() {
				defer wg.Done()
				resp, err := op(ctx, replica, c.TxID)
				if err == nil {
					successful.Add(1)
				}
				replyCh <- Result[T]{resp, err}
			}
			enterrors.GoWrapper(g, c.log)
		}

		wg.Wait()
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh
}

func (c *coordinator[T, R]) read(
	level int,
	ch <-chan Result[T],
	onResult onResult[T],
	onFlatten onFlatten[T, R],
	batchSize int,
) []R {
	required := level
	failures := make([]T, 0, level)
	successes := make([]T, 0, level)
	var replicaErrs []error
	for x := range ch {
		var err error
		var shouldDecreaseLevel bool
		successes, failures, shouldDecreaseLevel, err = onResult(x, successes, failures)
		if err != nil {
			replicaErrs = append(replicaErrs, err)
		}
		if shouldDecreaseLevel {
			level--
		}
		if level == 0 { // consistency level reached
			return onFlatten(batchSize, successes, nil)
		}
	}
	var finalErr error
	if level > 0 {
		joined := errors.Join(replicaErrs...)
		// If the upstream failure is already a "not enough replicas" error
		// (e.g. broadcast aborted and sent its own NotEnoughReplicasError),
		// surface it directly instead of nesting another wrapper.
		if errors.Is(joined, replicaerrors.ErrReplicas) {
			finalErr = joined
		} else {
			finalErr = replicaerrors.NewNotEnoughReplicasErrorWithCounts(required, required-level, joined)
		}
	}
	failures = append(failures, successes...)
	return onFlatten(batchSize, failures, finalErr)
}

// Push pushes updates to all replicas of a specific shard
func (c *coordinator[T, R]) Push(ctx context.Context,
	cl types.ConsistencyLevel,
	ask readyOp,
	com commitOp[T],
	onResult onResult[T],
	onFlatten onFlatten[T, R],
	batchSize int,
) ([]R, error) {
	options := c.Router.BuildRoutingPlanOptions(c.Shard, c.Shard, cl, "")
	writeRoutingPlan, err := c.Router.BuildWriteRoutingPlan(options)
	if err != nil {
		return nil, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}

	level := writeRoutingPlan.IntConsistencyLevel

	//nolint:govet // cancelling here would abort prepares that commitAll may still be consuming; the timeout bounds it
	ctxWithTimeout, _ := context.WithTimeout(ctx, defaultPrepareTimeout)
	c.log.WithFields(writeRoutingPlan.LogFields()).WithFields(logrus.Fields{
		"action":     "coordinator_push",
		"duration":   defaultPrepareTimeout,
		"level":      level,
		"class":      c.Class,
		"request_id": c.TxID,
	}).Debug("pushing write to resolved replica set")

	// create callback for metrics
	// the use of an immediately invoked function expression (IIFE) captures the start time
	// and returns the actual callback function.
	// The returned function is then called by commitAll once it knows how many
	// replicas have successfully committed
	callback := func() func(successful int) {
		start := time.Now()

		return func(successful int) {
			numReplicas := len(writeRoutingPlan.Replicas())

			if numReplicas == successful {
				c.metrics.IncWritesSucceedAll()
			} else if successful > 0 {
				c.metrics.IncWritesSucceedSome()
			} else {
				c.metrics.IncWritesFailed()
			}

			c.metrics.ObserveWriteDuration(time.Since(start))
		}
	}()

	nodeCh := c.broadcast(ctxWithTimeout, writeRoutingPlan.HostAddresses(), ask, level)

	// keeps the caller's values, never its cancellation: see defaultCommitTimeout
	//nolint:govet // deliberately outlives the caller; defaultCommitTimeout bounds it
	commitCtx, _ := context.WithTimeout(context.WithoutCancel(ctx), defaultCommitTimeout)
	commitCh := c.commitAll(commitCtx, nodeCh, com, callback)

	return c.read(level, commitCh, onResult, onFlatten, batchSize), nil
}

// Pull data from replica depending on consistency level, trying to reach level successful calls
// to op, while cycling through replicas for the coordinator's shard.
//
// Some invariants of this method (some callers depend on these):
// - Try the first fullread op on the directCandidate (if directCandidate is non-empty)
// - Only one successful fullread op will be forwarded to the caller
// - Query level replicas concurrently, and avoid querying more than level unless a replica fails or stalls
// - Only send up to level messages onto replyCh, exactly one per worker
// - Only send error messages on replyCh once it's unlikely we'll ever reach level successes
// - Never forward two replies from the same replica, so votes are always distinct
//
// Note that the first retry for a given host, may happen before c.pullBackOff.initial has passed
func (c *coordinator[T, any]) Pull(ctx context.Context,
	cl types.ConsistencyLevel,
	op readOp[T], directCandidate string,
	timeout time.Duration,
) (<-chan Result[T], int, error) {
	options := c.Router.BuildRoutingPlanOptions(c.Shard, c.Shard, cl, directCandidate)
	readRoutingPlan, err := c.Router.BuildReadRoutingPlan(options)
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := readRoutingPlan.IntConsistencyLevel
	hosts := readRoutingPlan.HostAddresses()
	replyCh := make(chan Result[T], level)
	f := func() {
		start := time.Now()
		var successful atomic.Int32

		defer func() {
			if int(successful.Load()) == level {
				c.metrics.IncReadsSucceedAll()
			} else if successful.Load() > 0 {
				c.metrics.IncReadsSucceedSome()
			} else {
				c.metrics.IncReadsFailed()
			}

			c.metrics.ObserveReadDuration(time.Since(start))
		}()

		// replicas not needed up front; a host is only taken, never returned, so none votes twice
		hostRetryQueue := make(chan hostRetry, len(hosts)-level)

		// put the "backups/fallbacks" on the retry queue
		for i := level; i < len(hosts); i++ {
			hostRetryQueue <- hostRetry{
				hosts[i],
				backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
			}
		}

		// kick off only level workers so that we avoid querying nodes unnecessarily
		wg := sync.WaitGroup{}
		wg.Add(level)
		for i := 0; i < level; i++ {
			// each worker owns its corresponding host (eg worker0 owns hosts[0],
			// worker1 owns hosts[1], etc). We want the fullRead to be tried on hosts[0]
			// because that will be the direct candidate (if a direct candidate was provided),
			// if we only used the retry queue then we would not have the guarantee that the
			// fullRead will be tried on hosts[0] first.
			own := hostRetry{
				hosts[i],
				backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
			}
			isFullReadWorker := i == 0 // first worker will perform the fullRead
			workerFunc := func() {
				defer wg.Done()
				c.pullWorker(ctx, op, own, isFullReadWorker, hostRetryQueue, replyCh, &successful, timeout)
			}
			enterrors.GoWrapper(workerFunc, c.log)
		}
		wg.Wait()
		// callers of this function rely on replyCh being closed
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, level, nil
}

// hostRetry tracks how long we should wait to retry this host again
type hostRetry struct {
	host           string
	currentBackOff backoff.BackOff
}

type hostResult[T any] struct {
	host string
	resp T
	err  error
}

// pullWorker serves one slot of a Pull, hedging a silent replica against an idle one
func (c *coordinator[T, any]) pullWorker(ctx context.Context,
	op readOp[T],
	own hostRetry,
	fullRead bool,
	retryQueue chan hostRetry,
	replyCh chan<- Result[T],
	successful *atomic.Int32,
	timeout time.Duration,
) {
	workerCtx, workerCancel := context.WithTimeout(ctx, timeout)
	defer workerCancel() // releases every attempt this worker stopped waiting for

	attempts := make(chan hostResult[T], cap(retryQueue)+1)

	inFlight := make([]hostRetry, 0, cap(retryQueue)+1)

	start := func(hr hostRetry, after time.Duration) {
		inFlight = append(inFlight, hr)
		g := func() {
			if after > 0 {
				timer := time.NewTimer(after)
				select {
				case <-timer.C:
				case <-workerCtx.Done():
					timer.Stop()
					return
				}
				timer.Stop()
			}
			resp, err := op(workerCtx, hr.host, fullRead)
			select {
			case attempts <- hostResult[T]{host: hr.host, resp: resp, err: err}:
			case <-workerCtx.Done():
			}
		}
		enterrors.GoWrapper(g, c.log)
	}

	settle := func(host string) (hostRetry, bool) {
		for i, hr := range inFlight {
			if hr.host != host {
				continue
			}
			inFlight = append(inFlight[:i], inFlight[i+1:]...)
			return hr, true
		}
		return hostRetry{}, false
	}

	takeIdleReplica := func() (hostRetry, bool) {
		select {
		case hr := <-retryQueue:
			return hr, true
		default:
			return hostRetry{}, false
		}
	}

	start(own, 0)

	hedgeDelay := c.pullHostHedgeDelay
	if hedgeDelay <= 0 { // a coordinator built without one must still hedge
		hedgeDelay = defaultPullHostHedgeDelay
	}
	hedge := time.NewTicker(hedgeDelay)
	defer hedge.Stop()

	var (
		lastResp T
		lastErr  error
	)
	for {
		select {
		case <-workerCtx.Done():
			if lastErr == nil {
				lastErr = workerCtx.Err()
			}
			replyCh <- Result[T]{lastResp, lastErr}
			return

		case res := <-attempts:
			hr, ok := settle(res.host)
			if res.err == nil {
				successful.Add(1)
				replyCh <- Result[T]{res.resp, nil}
				return
			}
			lastResp, lastErr = res.resp, res.err

			if idle, found := takeIdleReplica(); found {
				start(idle, 0)
			}
			if ok {
				if next := hr.currentBackOff.NextBackOff(); next != backoff.Stop {
					start(hr, next)
				}
			}
			if len(inFlight) == 0 {
				replyCh <- Result[T]{lastResp, lastErr}
				return
			}

		case <-hedge.C:
			if len(inFlight) == 0 {
				continue
			}
			if idle, found := takeIdleReplica(); found {
				start(idle, 0)
			}
		}
	}
}

// annotateReplicaErr prefixes err with the replica identifier when
// replica is non-empty.  It returns err unchanged when no identifier is
// available so that an empty prefix is never emitted.
func annotateReplicaErr(replica string, err error) error {
	if replica == "" {
		return err
	}
	return fmt.Errorf("replica %q: %w", replica, err)
}
