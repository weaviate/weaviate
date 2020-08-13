package classification

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
)

type runWorker struct {
	jobs         []search.Result
	successCount *int64
	errorCount   *int64
	ec           *errorCompounder
	classify     classifyItemFn
	kind         kind.Kind
	params       models.Classification
	filters      filters
	id           int
	workerCount  int
}

func (w *runWorker) addJob(job search.Result) {
	w.jobs = append(w.jobs, job)
}

func (w *runWorker) work(wg *sync.WaitGroup) {
	defer wg.Done()

	for i, item := range w.jobs {
		originalIndex := (i * w.workerCount) + w.id
		err := w.classify(item, originalIndex, w.kind, w.params, w.filters)
		if err != nil {
			w.ec.add(err)
			atomic.AddInt64(w.errorCount, 1)
		} else {
			atomic.AddInt64(w.successCount, 1)
		}
		fmt.Printf("worker %d has completed job %d\n", w.id, i)
	}
}

func newRunWorker(id int, workerCount int, rw *runWorkers) *runWorker {
	return &runWorker{
		successCount: rw.successCount,
		errorCount:   rw.errorCount,
		ec:           rw.ec,
		kind:         rw.kind,
		params:       rw.params,
		filters:      rw.filters,
		classify:     rw.classify,
	}
}

type runWorkers struct {
	workers      []*runWorker
	successCount *int64
	errorCount   *int64
	ec           *errorCompounder
	classify     classifyItemFn
	kind         kind.Kind
	params       models.Classification
	filters      filters
}

func newRunWorkers(amount int, classifyFn classifyItemFn, kind kind.Kind,
	params models.Classification, filters filters) *runWorkers {
	var successCount int64
	var errorCount int64

	rw := &runWorkers{
		workers:      make([]*runWorker, amount),
		successCount: &successCount,
		errorCount:   &errorCount,
		ec:           &errorCompounder{},
		classify:     classifyFn,
		kind:         kind,
		params:       params,
		filters:      filters,
	}

	for i := 0; i < amount; i++ {
		rw.workers[i] = newRunWorker(i, amount, rw)
	}

	return rw
}

func (ws *runWorkers) addJobs(jobs []search.Result) {
	for i, job := range jobs {
		ws.workers[i%len(ws.workers)].addJob(job)
	}
}

func (ws *runWorkers) work() runWorkerResults {
	var ()
	wg := &sync.WaitGroup{}
	for _, worker := range ws.workers {
		wg.Add(1)
		go worker.work(wg)
	}

	wg.Wait()

	return runWorkerResults{
		successCount: *ws.successCount,
		errorCount:   *ws.errorCount,
		err:          ws.ec.toError(),
	}
}

type runWorkerResults struct {
	successCount int64
	errorCount   int64
	err          error
}
