//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package asyncwriter

import (
	"context"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

type linkedBuff struct {
	buffer []byte
	next   *linkedBuff
}

type AsyncQueuedWriter struct {
	file         *os.File
	bufw         *bufWriter
	newData      chan bool
	finishedData semaphore.Weighted
	currBuff     *linkedBuff
	lastBuff     *linkedBuff
	lock         *sync.Mutex
	closeLock    *sync.RWMutex
	awaiting     atomic.Bool
}

func NewAsyncQueuedWriter(fileName string) *AsyncQueuedWriter {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	writer := &AsyncQueuedWriter{file: file, bufw: NewWriterSize(file, 32*1024)}
	writer.initWriterWorker()
	return writer
}

func NewAsyncQueuedWriterWithFile(file *os.File) *AsyncQueuedWriter {
	writer := &AsyncQueuedWriter{file: file, bufw: NewWriterSize(file, 32*1024)}
	writer.initWriterWorker()
	return writer
}

func (w *AsyncQueuedWriter) initWriterWorker() {
	w.newData = make(chan bool)
	w.finishedData = *semaphore.NewWeighted(1)
	w.currBuff = &linkedBuff{}
	w.lastBuff = w.currBuff
	w.lock = &sync.Mutex{}
	w.closeLock = &sync.RWMutex{}
	go func() {
		for {
			<-w.newData
			w.bufw.Write(w.currBuff.buffer)
			w.currBuff = w.currBuff.next
			if w.currBuff.next == nil && w.awaiting.Load() {
				w.awaiting.Store(false)
				w.finishedData.Release(1)
			}
		}
	}()
}

func (w *AsyncQueuedWriter) QueueData(buff []byte) {
	w.closeLock.RLock()
	defer w.closeLock.RUnlock()
	w.lock.Lock()
	w.lastBuff.buffer = buff
	w.lastBuff.next = &linkedBuff{}
	w.lastBuff = w.lastBuff.next
	w.lock.Unlock()
	w.newData <- true
}

func (w *AsyncQueuedWriter) Stat() (fs.FileInfo, error) {
	return w.file.Stat()
}

func (w *AsyncQueuedWriter) Flush() error {
	w.closeLock.Lock()
	defer w.closeLock.Unlock()

	for w.currBuff.next != nil {
		w.awaiting.Store(true)
		w.finishedData.Acquire(context.Background(), 1)
	}

	return w.bufw.Flush()
}

func (w *AsyncQueuedWriter) Close() error {
	w.closeLock.Lock()
	defer w.closeLock.Unlock()

	for w.currBuff.next != nil {
		w.awaiting.Store(true)
		w.finishedData.Acquire(context.Background(), 1)
	}

	if err := w.bufw.Flush(); err != nil {
		return err
	}

	return w.file.Close()
}
