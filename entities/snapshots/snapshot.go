package snapshots

import "time"

type Snapshot struct {
	StartedAt   time.Time
	CompletedAt time.Time

	ID    string
	Files []string
}

// type Backup struct {
// 	Events []BackupEvent
// }

// type BackupEvent struct {
// 	Time time.Time
// 	Msg  string
// }
