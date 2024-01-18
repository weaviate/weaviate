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

package backup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
	"golang.org/x/sync/errgroup"
)

// Op is the kind of a backup operation
type Op string

const (
	OpCreate  Op = "create"
	OpRestore Op = "restore"
)

var (
	errNoShardFound = errors.New("no shard found")
	errCannotCommit = errors.New("cannot commit")
	errMetaNotFound = errors.New("metadata not found")
	errUnknownOp    = errors.New("unknown backup operation")
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

// selector is used to select participant nodes
type selector interface {
	// Shards gets all nodes on which this class is sharded
	Shards(ctx context.Context, class string) ([]string, error)
	// ListClasses returns a list of all existing classes
	// This will be needed if user doesn't include any classes
	ListClasses(ctx context.Context) []string

	// Backupable returns whether all given class can be backed up.
	Backupable(_ context.Context, classes []string) error
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
	selector     selector
	client       client
	log          logrus.FieldLogger
	nodeResolver nodeResolver

	// state
	Participants map[string]participantStatus
	descriptor   *backup.DistributedBackupDescriptor
	shardSyncChan

	// timeouts
	timeoutNodeDown    time.Duration
	timeoutQueryStatus time.Duration
	timeoutCanCommit   time.Duration
	timeoutNextRound   time.Duration
}

// newcoordinator creates an instance which coordinates distributed BRO operations among many shards.
func newCoordinator(
	selector selector,
	client client,
	log logrus.FieldLogger,
	nodeResolver nodeResolver,
) *coordinator {
	return &coordinator{
		selector:           selector,
		client:             client,
		log:                log,
		nodeResolver:       nodeResolver,
		Participants:       make(map[string]participantStatus, 16),
		timeoutNodeDown:    _TimeoutNodeDown,
		timeoutQueryStatus: _TimeoutQueryStatus,
		timeoutCanCommit:   _TimeoutCanCommit,
		timeoutNextRound:   _NextRoundPeriod,
	}
}

// Backup coordinates a distributed backup among participants
func (c *coordinator) Backup(ctx context.Context, store coordStore, req *Request) error {
	req.Method = OpCreate
	groups, err := c.groupByShard(ctx, req.Classes)
	if err != nil {
		return err
	}
	// make sure there is no active backup
	if prevID := c.lastOp.renew(req.ID, store.HomeDir()); prevID != "" {
		return fmt.Errorf("backup %s already in progress", prevID)
	}

	c.descriptor = &backup.DistributedBackupDescriptor{
		StartedAt:     time.Now().UTC(),
		Status:        backup.Started,
		ID:            req.ID,
		Nodes:         groups,
		Version:       Version,
		ServerVersion: config.ServerVersion,
	}

	for key := range c.Participants {
		delete(c.Participants, key)
	}

	nodes, err := c.canCommit(ctx, req)
	if err != nil {
		c.lastOp.reset()
		return err
	}

	if err := store.PutMeta(ctx, GlobalBackupFile, c.descriptor); err != nil {
		c.lastOp.reset()
		return fmt.Errorf("cannot init meta file: %w", err)
	}

	statusReq := StatusRequest{
		Method:  OpCreate,
		ID:      req.ID,
		Backend: req.Backend,
	}

	go func() {
		defer c.lastOp.reset()
		ctx := context.Background()
		c.commit(ctx, &statusReq, nodes, false)
		if err := store.PutMeta(ctx, GlobalBackupFile, c.descriptor); err != nil {
			c.log.WithField("action", OpCreate).
				WithField("backup_id", req.ID).Errorf("put_meta: %v", err)
		}
	}()

	return nil
}

// Restore coordinates a distributed restoration among participants
func (c *coordinator) Restore(
	ctx context.Context,
	store coordStore,
	req *Request,
	desc *backup.DistributedBackupDescriptor,
) error {
	req.Method = OpRestore
	// make sure there is no active backup
	if prevID := c.lastOp.renew(desc.ID, store.HomeDir()); prevID != "" {
		return fmt.Errorf("restoration %s already in progress", prevID)
	}

	for key := range c.Participants {
		delete(c.Participants, key)
	}
	c.descriptor = desc.ResetStatus()

	nodes, err := c.canCommit(ctx, req)
	if err != nil {
		c.lastOp.reset()
		return err
	}

	// initial put so restore status is immediately available
	if err := store.PutMeta(ctx, GlobalRestoreFile, c.descriptor); err != nil {
		c.lastOp.reset()
		req := &AbortRequest{Method: OpRestore, ID: desc.ID, Backend: req.Backend}
		c.abortAll(ctx, req, nodes)
		return fmt.Errorf("put initial metadata: %w", err)
	}

	statusReq := StatusRequest{Method: OpRestore, ID: desc.ID, Backend: req.Backend}
	go func() {
		defer c.lastOp.reset()
		ctx := context.Background()
		c.commit(ctx, &statusReq, nodes, true)
		if err := store.PutMeta(ctx, GlobalRestoreFile, c.descriptor); err != nil {
			c.log.WithField("action", OpRestore).WithField("backup_id", desc.ID).Errorf("put_meta: %v", err)
		}
	}()

	return nil
}

func (c *coordinator) OnStatus(ctx context.Context, store coordStore, req *StatusRequest) (*Status, error) {
	// check if backup is still active
	st := c.lastOp.get()
	if st.ID == req.ID {
		return &Status{Path: st.Path, StartedAt: st.Starttime, Status: st.Status}, nil
	}
	filename := GlobalBackupFile
	if req.Method == OpRestore {
		filename = GlobalRestoreFile
	}
	// The backup might have been already created.
	meta, err := store.Meta(ctx, filename)
	if err != nil {
		path := fmt.Sprintf("%s/%s", req.ID, filename)
		return nil, fmt.Errorf("coordinator cannot get status: %w: %q: %v", errMetaNotFound, path, err)
	}

	return &Status{
		Path:        store.HomeDir(),
		StartedAt:   meta.StartedAt,
		CompletedAt: meta.CompletedAt,
		Status:      meta.Status,
		Err:         meta.Error,
	}, nil
}

// canCommit asks candidates if they agree to participate in DBRO
// It returns and error if any candidates refuses to participate
func (c *coordinator) canCommit(ctx context.Context, req *Request) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutCanCommit)
	defer cancel()

	type nodeHost struct {
		node, host string
	}

	type pair struct {
		n nodeHost
		r *Request
	}

	id := c.descriptor.ID
	nodeMapping := c.descriptor.NodeMapping
	groups := c.descriptor.Nodes

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(_MaxNumberConns)
	reqChan := make(chan pair)
	g.Go(func() error {
		defer close(reqChan)
		for node, gr := range groups {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// If we have a nodeMapping with the node name from the backup, replace the node with the new one
			node = c.descriptor.ToMappedNodeName(node)

			host, found := c.nodeResolver.NodeHostname(node)
			if !found {
				return fmt.Errorf("cannot resolve hostname for %q", node)
			}

			reqChan <- pair{
				nodeHost{node, host},
				&Request{
					Method:      req.Method,
					ID:          id,
					Backend:     req.Backend,
					Classes:     gr.Classes,
					Duration:    _BookingPeriod,
					NodeMapping: nodeMapping,
					Compression: req.Compression,
				},
			}
		}
		return nil
	})

	mutex := sync.RWMutex{}
	nodes := make(map[string]string, len(groups))
	for pair := range reqChan {
		pair := pair
		g.Go(func() error {
			resp, err := c.client.CanCommit(ctx, pair.n.host, pair.r)
			if err == nil && resp.Timeout == 0 {
				err = fmt.Errorf("%w : %v", errCannotCommit, resp.Err)
			}
			if err != nil {
				return fmt.Errorf("node %q: %w", pair.n, err)
			}
			mutex.Lock()
			nodes[pair.n.node] = pair.n.host
			mutex.Unlock()
			return nil
		})
	}
	abortReq := &AbortRequest{Method: req.Method, ID: id, Backend: req.Backend}
	if err := g.Wait(); err != nil {
		c.abortAll(ctx, abortReq, nodes)
		return nil, err
	}
	return nodes, nil
}

// commit tells each participant to commit its backup operation
// It stores the final result in the provided backend
func (c *coordinator) commit(ctx context.Context,
	req *StatusRequest,
	node2Addr map[string]string,
	toleratePartialFailure bool,
) {
	// create a new copy for commitAll and queryAll to mutate
	node2Host := make(map[string]string, len(node2Addr))
	for k, v := range node2Addr {
		node2Host[k] = v
	}
	nFailures := c.commitAll(ctx, req, node2Host)
	retryAfter := c.timeoutNextRound / 5 // 2s for first time
	canContinue := len(node2Host) > 0 && (toleratePartialFailure || nFailures == 0)
	for canContinue {
		<-time.After(retryAfter)
		retryAfter = c.timeoutNextRound
		nFailures += c.queryAll(ctx, req, node2Host)
		canContinue = len(node2Host) > 0 && (toleratePartialFailure || nFailures == 0)
	}
	if !toleratePartialFailure && nFailures > 0 {
		req := &AbortRequest{Method: req.Method, ID: req.ID, Backend: req.Backend}
		c.abortAll(context.Background(), req, node2Addr)
	}
	c.descriptor.CompletedAt = time.Now().UTC()
	status := backup.Success
	reason := ""
	groups := c.descriptor.Nodes
	for node, p := range c.Participants {
		st := groups[c.descriptor.ToOriginalNodeName(node)]
		st.Status, st.Error = p.Status, p.Reason
		if p.Status != backup.Success {
			status = backup.Failed
			reason = p.Reason
		}
		groups[node] = st
	}
	c.descriptor.Status = status
	c.descriptor.Error = reason
}

// queryAll queries all participant and store their statuses internally
//
// It returns the number of failed node backups
func (c *coordinator) queryAll(ctx context.Context, req *StatusRequest, nodes map[string]string) int {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutQueryStatus)
	defer cancel()

	rs := make([]partialStatus, len(nodes))
	g, ctx := errgroup.WithContext(ctx)
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
		st := c.Participants[r.node]
		if r.err == nil {
			st.LastTime, st.Status, st.Reason = now, r.Status, r.Err
			if r.Status == backup.Success {
				delete(nodes, r.node)
			}
			if r.Status == backup.Failed {
				delete(nodes, r.node)
				n++
			}
		} else if now.Sub(st.LastTime) > c.timeoutNodeDown {
			n++
			st.Status = backup.Failed
			st.Reason = "might be down:" + r.err.Error()
			delete(nodes, r.node)
		}
		c.Participants[r.node] = st
	}
	return n
}

// commitAll tells all participants to proceed with their backup operations
// It returns the number of failures
func (c *coordinator) commitAll(ctx context.Context, req *StatusRequest, nodes map[string]string) int {
	type pair struct {
		node string
		err  error
	}
	errChan := make(chan pair)
	aCounter := int64(len(nodes))
	g, ctx := errgroup.WithContext(ctx)
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
		st := c.Participants[x.node]
		st.Status = backup.Failed
		st.Reason = "might be down:" + x.err.Error()
		c.Participants[x.node] = st
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
func (c *coordinator) groupByShard(ctx context.Context, classes []string) (nodeMap, error) {
	m := make(nodeMap, 32)
	for _, cls := range classes {
		nodes, err := c.selector.Shards(ctx, cls)
		if err != nil {
			return nil, fmt.Errorf("class %q: %w", cls, errNoShardFound)
		}
		for _, node := range nodes {
			nd, ok := m[node]
			if !ok {
				nd = &backup.NodeDescriptor{Classes: make([]string, 0, 5)}
			}
			nd.Classes = append(nd.Classes, cls)
			m[node] = nd
		}
	}
	return m, nil
}

// partialStatus tracks status of a single backup operation
type partialStatus struct {
	node string
	*StatusResponse
	err error
}
