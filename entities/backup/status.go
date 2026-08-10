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

type Status string

const (
	Started      Status = "STARTED"
	Transferring Status = "TRANSFERRING"
	Transferred  Status = "TRANSFERRED"
	Finalizing   Status = "FINALIZING" // Schema apply in progress - cancellation blocked
	Success      Status = "SUCCESS"
	Cancelling   Status = "CANCELLING" // Cancellation in progress - claimed by a coordinator
	Cancelled    Status = "CANCELED"
	Failed       Status = "FAILED"
)

// IsCancellation reports whether the operation is being cancelled or has been.
// Testing only for Cancelled would treat an in-flight cancel as live.
func (s Status) IsCancellation() bool {
	return s == Cancelling || s == Cancelled
}

type CreateMeta struct {
	Path   string
	Status Status
}

type RestoreMeta struct {
	Path   string
	Status Status
}
