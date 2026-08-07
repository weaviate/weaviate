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

package asyncrep_battle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
)

// opState is the writer's acked-only model of one id.
type opState struct {
	Exists  bool
	Version int64
	Tainted bool
}

// churnWriter issues upserts and deletes through its own HTTP clients (never
// the process-global test client). Each id has exactly one issuing goroutine
// and ops per id are sequential, so the known equal-millisecond LWW tie gap
// cannot be tripped by construction. Only definitely-acked ops update the
// model; ambiguous outcomes taint the id, excluding it from per-id probes.
type churnWriter struct {
	class string
	ids   []strfmt.UUID
	prof  profile

	mu      sync.Mutex
	model   map[strfmt.UUID]opState
	targets []string

	acked atomic.Int64
	errs  atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
	client *http.Client
}

func newChurnWriter(class string, p profile, targets []string) *churnWriter {
	ids := make([]strfmt.UUID, p.idSpace)
	for i := range ids {
		ids[i] = strfmt.UUID(uuid.NewString())
	}
	return &churnWriter{
		class:   class,
		ids:     ids,
		prof:    p,
		model:   make(map[strfmt.UUID]opState, p.idSpace),
		targets: targets,
		stopCh:  make(chan struct{}),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (w *churnWriter) start() {
	per := len(w.ids) / w.prof.writerGoroutines
	for g := 0; g < w.prof.writerGoroutines; g++ {
		lo, hi := g*per, (g+1)*per
		if g == w.prof.writerGoroutines-1 {
			hi = len(w.ids)
		}
		w.wg.Add(1)
		go w.run(g, w.ids[lo:hi])
	}
}

func (w *churnWriter) run(g int, owned []strfmt.UUID) {
	defer w.wg.Done()
	rng := rand.New(rand.NewSource(int64(g) + time.Now().UnixNano()))
	for {
		select {
		case <-w.stopCh:
			return
		case <-time.After(w.prof.opInterval):
		}
		id := owned[rng.Intn(len(owned))]

		w.mu.Lock()
		st := w.model[id]
		target := w.targets[rng.Intn(len(w.targets))]
		w.mu.Unlock()
		if st.Tainted {
			continue
		}

		if rng.Intn(100) < 80 {
			w.upsert(target, id, st.Version+1, g, st.Exists)
		} else {
			w.delete(target, id)
		}
	}
}

// upsert POSTs a create for ids the model says are absent (REST PUT is
// update-only and 404s on unknown ids) and PUTs an overwrite otherwise.
func (w *churnWriter) upsert(target string, id strfmt.UUID, ver int64, g int, exists bool) {
	body, _ := json.Marshal(map[string]interface{}{
		"class": w.class,
		"id":    id,
		"properties": map[string]interface{}{
			"contents": fmt.Sprintf("battle-%s", id),
			"ver":      ver,
			"wid":      fmt.Sprintf("g%d", g),
		},
	})
	method := http.MethodPost
	url := fmt.Sprintf("http://%s/v1/objects?consistency_level=ONE", target)
	if exists {
		method = http.MethodPut
		url = fmt.Sprintf("http://%s/v1/objects/%s/%s?consistency_level=ONE", target, w.class, id)
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		w.errs.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		w.taint(id)
		return
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		w.applyAck(id, opState{Exists: true, Version: ver})
	case !exists && resp.StatusCode == http.StatusUnprocessableEntity:
		// An earlier ambiguous create landed; flip to exists so the next op PUTs.
		w.applyAck(id, opState{Exists: true, Version: ver})
	default:
		w.errs.Add(1)
	}
}

func (w *churnWriter) delete(target string, id strfmt.UUID) {
	url := fmt.Sprintf("http://%s/v1/objects/%s/%s?consistency_level=ONE", target, w.class, id)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		w.errs.Add(1)
		return
	}
	resp, err := w.client.Do(req)
	if err != nil {
		w.taint(id)
		return
	}
	defer resp.Body.Close()
	// 404 = already absent; both outcomes leave the id definitely deleted.
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		w.applyAck(id, opState{Exists: false})
		return
	}
	w.errs.Add(1)
}

func (w *churnWriter) applyAck(id strfmt.UUID, st opState) {
	w.acked.Add(1)
	w.mu.Lock()
	prev := w.model[id]
	st.Tainted = prev.Tainted
	if st.Exists && st.Version < prev.Version {
		st.Version = prev.Version
	}
	w.model[id] = st
	w.mu.Unlock()
}

func (w *churnWriter) taint(id strfmt.UUID) {
	w.errs.Add(1)
	w.mu.Lock()
	st := w.model[id]
	st.Tainted = true
	w.model[id] = st
	w.mu.Unlock()
}

// setTargets swaps the alive-node URI list; called around every node cycle.
func (w *churnWriter) setTargets(uris []string) {
	w.mu.Lock()
	w.targets = uris
	w.mu.Unlock()
}

func (w *churnWriter) stop() (acked, errs int64) {
	close(w.stopCh)
	w.wg.Wait()
	return w.acked.Load(), w.errs.Load()
}

// sample returns up to nLive existing and nDeleted deleted untainted ids.
func (w *churnWriter) sample(nLive, nDeleted int) (live, deleted []strfmt.UUID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, st := range w.model {
		if st.Tainted {
			continue
		}
		if st.Exists && len(live) < nLive {
			live = append(live, id)
		}
		if !st.Exists && len(deleted) < nDeleted {
			deleted = append(deleted, id)
		}
		if len(live) >= nLive && len(deleted) >= nDeleted {
			break
		}
	}
	return live, deleted
}
