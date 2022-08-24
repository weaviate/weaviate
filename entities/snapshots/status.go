package snapshots

const (
	CreateStarted      CreateStatus = "STARTED"
	CreateTransferring CreateStatus = "TRANSFERRING"
	CreateTransferred  CreateStatus = "TRANSFERRED"
	CreateSuccess      CreateStatus = "SUCCESS"
	CreateFailed       CreateStatus = "FAILED"
)

const (
	RestoreStarted      RestoreStatus = "STARTED"
	RestoreTransferring RestoreStatus = "TRANSFERRING"
	RestoreTransferred  RestoreStatus = "TRANSFERRED"
	RestoreSuccess      RestoreStatus = "SUCCESS"
	RestoreFailed       RestoreStatus = "FAILED"
)

type (
	CreateStatus  string
	RestoreStatus string
)

type CreateMeta struct {
	Path   string
	Status CreateStatus
}

type RestoreMeta struct {
	Path   string
	Status RestoreStatus
}
