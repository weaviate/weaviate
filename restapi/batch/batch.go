package batch

import "github.com/creativesoftwarefdn/weaviate/restapi/state"

// New creates all REST api handlers for batching things and actions
func New(appState *state.State) *Batch {
	return &Batch{
		appState: appState,
	}
}

// Batch provides various Handlers around batching things and actions
type Batch struct {
	appState *state.State
}
