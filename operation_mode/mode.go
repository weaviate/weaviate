package operation_mode

import (
	log "github.com/sirupsen/logrus"
	"sync"
)

type OperationMode struct {
	sync.Mutex
	errors []error
}

// Create a new OperationMode.
// Weaviate can run in a 'reduced' functionality state, when the names/keywords of the classes/properties cannot
// be grounded in the Contextionary.
//
// NOTE: Don't copy a OperationsMode struct! This will break the interface, because it stores the synchronization state
// inside the struct.
func New() OperationMode {
	return OperationMode{errors: nil}
}

func (m *OperationMode) HasErrors() bool {
	m.Lock()
	defer m.Unlock()

	return len(m.errors) > 0
}

func (m *OperationMode) Errors() []error {
	m.Lock()
	defer m.Unlock()

	// Copy to prevent racy access.
	output := make([]error, len(m.errors))
	copy(output, m.errors)

	return output
}

func (m *OperationMode) AddError(err error) {
	m.Lock()
	defer m.Unlock()

	m.errors = append(m.errors, err)
}

func (m *OperationMode) RemoveError(err error) {
	m.Lock()
	defer m.Unlock()

	for i, opErr := range m.errors {
		if opErr == err {
			// remove the error
			copy(m.errors[i:], m.errors[i+1:])
			m.errors[len(m.errors)-1] = nil // or the zero value of T
			m.errors = m.errors[:len(m.errors)-1]
			return
		}
	}

	log.Warn("Removed an error that was not in the list of errors!")
}
