//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
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

type nodeMap map[string]backup.NodeDescriptor

// participantStatus tracks status of a participant in a DBRO
type participantStatus struct {
	Status   backup.Status
	Lasttime time.Time
	Reason   string
}

// selector is used to select participant nodes
type selector interface {
	// Shards gets all nodes on which this class is sharded
	Shards(ctx context.Context, class string) []string
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
	descriptor   backup.DistributedBackupDescriptor
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
	groups, err := c.groupByShard(ctx, req.Classes)
	if err != nil {
		return err
	}
	c.descriptor = backup.DistributedBackupDescriptor{
		StartedAt:     time.Now().UTC(),
		Status:        backup.Started,
		ID:            req.ID,
		Backend:       req.Backend,
		Nodes:         groups,
		Version:       Version,
		ServerVersion: config.ServerVersion,
	}

	// make sure there is no active backup
	if prevID := c.lastOp.renew(req.ID, time.Now(), store.HomeDir()); prevID != "" {
		return fmt.Errorf("backup %s already in progress", prevID)
	}
	nodes, err := c.canCommit(ctx, OpCreate)
	if err != nil {
		c.lastOp.reset()
		return err
	}

	statusReq := StatusRequest{
		Method:  OpCreate,
		ID:      req.ID,
		Backend: req.Backend,
	}

	go func() {
		defer c.lastOp.reset()
		ctx := context.Background()
		c.commit(ctx, &statusReq, nodes)
		if err := store.PutGlobalMeta(ctx, &c.descriptor); err != nil {
			c.log.WithField("action", OpCreate).
				WithField("backup_id", req.ID).Errorf("put_meta: %v", err)
		}
	}()

	return nil
}

// Restore coordinates a distributed restoration among participants
func (c *coordinator) Restore(ctx context.Context, req *backup.DistributedBackupDescriptor) error {
	c.descriptor = *req
	nodes, err := c.canCommit(ctx, OpRestore)
	if err != nil {
		return err
	}

	statusReq := StatusRequest{
		Method:  OpRestore,
		ID:      req.ID,
		Backend: req.Backend,
	}

	go func() {
		c.commit(context.Background(), &statusReq, nodes)
	}()

	return nil
}

// canCommit asks candidates if they agree to participate in DBRO
// It returns and error if any candidates refuses to participate
func (c *coordinator) canCommit(ctx context.Context, method Op) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutCanCommit)
	defer cancel()
	type pair struct {
		n string
		r *Request
	}

	id, backend := c.descriptor.ID, c.descriptor.Backend
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

			// TODO: this is where nodeResolver can be used to find node hostname.
			//       once found, it can be passed to pair below, rather than `node`
			//host, found := c.nodeResolver.NodeHostname(node)
			//if !found {
			//	return fmt.Errorf("failed to find hostname for node %q", node)
			//}

			reqChan <- pair{node, &Request{
				Method:   method,
				ID:       id,
				Backend:  backend,
				Classes:  gr.Classes,
				Duration: _BookingPeriod,
			}}
		}
		return nil
	})

	mutex := sync.RWMutex{}
	nodes := make(map[string]struct{}, len(groups))
	for pair := range reqChan {
		pair := pair
		g.Go(func() error {
			resp, err := c.client.CanCommit(ctx, pair.n, pair.r)
			if err == nil && resp.Timeout == 0 {
				err = errCannotCommit
			}
			if err != nil {
				return fmt.Errorf("node %q: %w", pair.n, err)
			}
			mutex.Lock()
			nodes[pair.n] = struct{}{}
			mutex.Unlock()
			return nil
		})
	}
	req := &AbortRequest{Method: method, ID: id, Backend: backend}
	if err := g.Wait(); err != nil {
		c.abortAll(ctx, req, nodes)
		return nil, err
	}
	return nodes, nil
}

// commit tells each participant to commit its backup operation
// It stores the final result in the provided backend
func (c *coordinator) commit(ctx context.Context, req *StatusRequest, nodes map[string]struct{}) {
	c.commitAll(ctx, req, nodes)

	for len(nodes) > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.timeoutNextRound):
			c.queryAll(ctx, req, nodes)
		}
	}
	c.descriptor.CompletedAt = time.Now().UTC()
	status := backup.Success
	reason := ""
	groups := c.descriptor.Nodes
	for node, p := range c.Participants {
		st := groups[node]
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
// It returns the number of remaining nodes to query in the next round
func (c *coordinator) queryAll(ctx context.Context, req *StatusRequest, nodes map[string]struct{}) int {
	ctx, cancel := context.WithTimeout(ctx, c.timeoutQueryStatus)
	defer cancel()

	rs := make([]partialStatus, len(nodes))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(_MaxNumberConns)
	i := 0
	for node := range nodes {
		j := i
		rs[j].node = node
		g.Go(func() error {
			rs[j].StatusResponse, rs[j].err = c.client.Status(ctx, rs[j].node, req)
			return nil
		})
		i++
	}
	g.Wait()
	now := time.Now()
	for _, r := range rs {
		st := c.Participants[r.node]
		if r.err == nil {
			st.Lasttime, st.Status, st.Reason = now, r.Status, r.Err
			if r.Status == backup.Success || r.Status == backup.Failed {
				delete(nodes, r.node)
			}
		} else if now.Sub(st.Lasttime) > c.timeoutNodeDown {
			st.Status = backup.Failed
			st.Reason = "might be down:" + r.err.Error()
			delete(nodes, r.node)
		}
		c.Participants[r.node] = st
	}
	return len(nodes)
}

// commitAll tells all participants to proceed with their backup operations
func (c *coordinator) commitAll(ctx context.Context, req *StatusRequest, nodes map[string]struct{}) {
	type pair struct {
		node string
		err  error
	}
	errChan := make(chan pair)
	aCounter := int64(len(nodes))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(_MaxNumberConns)
	for node := range nodes {
		node := node
		g.Go(func() error {
			defer func() {
				if atomic.AddInt64(&aCounter, -1) == 0 {
					close(errChan)
				}
			}()
			err := c.client.Commit(ctx, node, req)
			if err != nil {
				errChan <- pair{node, err}
			}
			return nil
		})
	}

	for x := range errChan {
		st := c.Participants[x.node]
		st.Status = backup.Failed
		st.Reason = "might be down:" + x.err.Error()
		c.Participants[x.node] = st
		c.log.WithField("action", req.Method).
			WithField("backup_id", req.ID).
			WithField("node", x.node).Error(x.err)
		delete(nodes, x.node)
		continue
	}
}

// abortAll tells every node to abort transaction
func (c *coordinator) abortAll(ctx context.Context, req *AbortRequest, nodes map[string]struct{}) {
	for node := range nodes {
		if err := c.client.Abort(ctx, node, req); err != nil {
			c.log.WithField("action", req.Method).
				WithField("backup_id", req.ID).
				WithField("node", node).Errorf("abort %v", err)
		}
	}
}

// groupByShard returns classes group by nodes
func (c *coordinator) groupByShard(ctx context.Context, classes []string) (nodeMap, error) {
	m := make(nodeMap, 32)
	for _, cls := range classes {
		nodes := c.selector.Shards(ctx, cls)
		if len(nodes) == 0 {
			return nil, fmt.Errorf("class %q: %w", cls, errNoShardFound)
		}
		for _, node := range nodes {
			nd, ok := m[node]
			if !ok {
				nd = backup.NodeDescriptor{Classes: make([]string, 0, 5)}
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
