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

package semaphore

type Semaphore struct {
	tokens chan struct{}
}

func NewSemaphore(n int) *Semaphore {
	return &Semaphore{
		tokens: make(chan struct{}, n),
	}
}

func (s *Semaphore) Acquire() {
	s.tokens <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.tokens
}
