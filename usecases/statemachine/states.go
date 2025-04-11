package statemachine

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// State represents the possible states of the system
type State string

// System states
const (
	StatePowerOff    State = "power-off"
	StateStartup     State = "startup"
	StateRunning     State = "running"
	StateBackup      State = "backup"
	StatePreShutdown State = "pre-shutdown"
	StateShutdown    State = "shutdown"
)

// Transition represents a state change with callbacks
type Transition struct {
	From      State
	To        State
	Timestamp time.Time
	Reason    string
}

// CallbackFunc is the function signature for state change callbacks
type CallbackFunc func(from, to State) error

// Component registration info
type componentInfo struct {
	name              string
	stateCallbacks    map[State]CallbackFunc
	allCallback       CallbackFunc
	coordinatedStates map[State]bool // States where component participates in coordination
	currentState      State          // Track the current state of each component
	readySignaled     map[State]bool // Track which states the component has already signaled ready for
}

// Module-level variables
var (
	globalState          atomic.Value
	mu                   sync.RWMutex
	history              []Transition
	components           = make(map[string]*componentInfo)
	allowedTransitions   = make(map[State][]State)
	registeredForState   = make(map[State][]string)      // Tracks which components are registered for each state
	stateCoordination    = make(map[State]*coordination) // Coordination info for each state
	transitionInProgress atomic.Bool
	initialized          atomic.Bool
	log                  = logrus.New()                // Flag for monitor mode
	stateWaiters         = make(map[State]*sync.Cond) // For waiting on a specific state
	stateWaitersMu       sync.Mutex                   // Mutex for stateWaiters
)

// coordination tracks the coordination for a specific state
type coordination struct {
	wg        sync.WaitGroup
	mu        sync.Mutex
	completed map[string]bool
}

func init() {
	// Initialize with power-off state
	globalState.Store(StatePowerOff)

	// Configure default logger
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// Define allowed state transitions
	allowedTransitions[StatePowerOff] = []State{StateStartup}
	allowedTransitions[StateStartup] = []State{StateRunning}
	allowedTransitions[StateRunning] = []State{StateBackup, StatePreShutdown}
	allowedTransitions[StateBackup] = []State{StateRunning, StatePreShutdown}
	allowedTransitions[StatePreShutdown] = []State{StateShutdown}
	allowedTransitions[StateShutdown] = []State{StatePowerOff} // Complete the cycle

	// Initialize coordination for each state
	allStates := []State{StatePowerOff, StateStartup, StateRunning, StateBackup, StatePreShutdown, StateShutdown}
	for _, state := range allStates {
		stateCoordination[state] = &coordination{
			completed: make(map[string]bool),
		}
		stateWaiters[state] = sync.NewCond(&stateWaitersMu)
	}



	initialized.Store(true)
}


// RegisterComponent registers a new component with the state machine
func RegisterComponent(name string) error {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := components[name]; exists {
		return fmt.Errorf("component %s already registered", name)
	}

	component := &componentInfo{
		name:              name,
		stateCallbacks:    make(map[State]CallbackFunc),
		coordinatedStates: make(map[State]bool),
		currentState:      StatePowerOff,        // Initially all components are in power-off state
		readySignaled:     make(map[State]bool), // Track which states have been signaled ready
	}

	components[name] = component
	log.WithFields(logrus.Fields{
		"component": name,
	}).Info("Component registered")
	return nil
}

// OnStateChange registers a callback for a specific state
func OnStateChange(componentName string, state State, callback CallbackFunc) error {
	mu.Lock()
	defer mu.Unlock()

	component, exists := components[componentName]
	if !exists {
		return fmt.Errorf("component %s not registered", componentName)
	}

	component.stateCallbacks[state] = callback

	// Track which components are registered for each state
	found := false
	for _, name := range registeredForState[state] {
		if name == componentName {
			found = true
			break
		}
	}

	if !found {
		registeredForState[state] = append(registeredForState[state], componentName)
	}

	log.WithFields(logrus.Fields{
		"component": componentName,
		"state":     state,
	}).Info("Component registered callback for state")
	return nil
}

// OnAnyStateChange registers a callback for any state change
func OnAnyStateChange(componentName string, callback CallbackFunc) error {
	mu.Lock()
	defer mu.Unlock()

	component, exists := components[componentName]
	if !exists {
		return fmt.Errorf("component %s not registered", componentName)
	}

	component.allCallback = callback
	log.WithFields(logrus.Fields{
		"component": componentName,
	}).Info("Component registered callback for all state changes")
	return nil
}

// RegisterForCoordination registers a component to participate in coordination for a state
// This means the system will wait for this component during this state transition
func RegisterForCoordination(componentName string, state State) error {
	mu.Lock()
	defer mu.Unlock()

	component, exists := components[componentName]
	if !exists {
		return fmt.Errorf("component %s not registered", componentName)
	}

	// Mark this component as participating in coordination for this state
	component.coordinatedStates[state] = true
	log.WithFields(logrus.Fields{
		"component": componentName,
		"state":     state,
	}).Info("Component registered for coordination")

	return nil
}

// SignalReady indicates that a component is in a specific state
// If the component is registered for coordination, it also counts as coordination completion
func SignalReady(componentName string, state State) error {
	mu.Lock()

	component, exists := components[componentName]
	if !exists {
		mu.Unlock()
		return fmt.Errorf("component %s not registered", componentName)
	}

	// Check if we're actually in this state at a global level
	currentGlobalState := CurrentState()
	if currentGlobalState != state {
		mu.Unlock()
		log.WithFields(logrus.Fields{
			"component":       componentName,
			"requested_state": state,
			"current_state":   currentGlobalState,
		}).Error("Component signaling ready for a state we aren't transitioning to")
		return fmt.Errorf("cannot signal ready for state %s when system is in state %s", state, currentGlobalState)
	}

	// Check for double-ready signal
	if component.readySignaled[state] {
		mu.Unlock()
		log.WithFields(logrus.Fields{
			"component": componentName,
			"state":     state,
		}).Warn("Component signaled ready twice for the same state")
		return fmt.Errorf("component %s already signaled ready for state %s", componentName, state)
	}

	// Mark as signaled
	component.readySignaled[state] = true

	// Update the component's current state
	prevState := component.currentState
	component.currentState = state
	log.WithFields(logrus.Fields{
		"component": componentName,
		"from":      prevState,
		"to":        state,
	}).Info("Component state changed")

	// Check if component is registered for coordination
	isCoordinating := component.coordinatedStates[state]
	mu.Unlock()

	// If component participates in coordination for this state, signal completion
	if isCoordinating {
		return SignalCoordinationComplete(componentName, state)
	}

	return nil
}

// SignalCoordinationComplete indicates that a component has completed its work for a state
func SignalCoordinationComplete(componentName string, state State) error {
	mu.RLock()
	component, exists := components[componentName]
	if !exists {
		mu.RUnlock()
		return fmt.Errorf("component %s not registered", componentName)
	}

	// Check if component is registered for coordination
	if !component.coordinatedStates[state] {
		mu.RUnlock()
		return fmt.Errorf("component %s not registered for coordination in state %s", componentName, state)
	}
	mu.RUnlock()

	// Update component's state if needed
	mu.Lock()
	if component.currentState != state {
		prevState := component.currentState
		component.currentState = state
		log.WithFields(logrus.Fields{
			"component": componentName,
			"from":      prevState,
			"to":        state,
		}).Info("Component state changed during coordination")
	}
	mu.Unlock()

	// Mark as completed and decrement wait group
	coordination := stateCoordination[state]
	coordination.mu.Lock()
	defer coordination.mu.Unlock()

	// Check if this component has already signaled completion
	if coordination.completed[componentName] {
		log.WithFields(logrus.Fields{
			"component": componentName,
			"state":     state,
		}).Warn("Component signaled coordination complete twice")
		return nil // Don't decrement the wait group again
	}

	coordination.completed[componentName] = true

	// Get current state to check if we're actually in a transition
	currentGlobalState := CurrentState()
	if currentGlobalState != state {
		log.WithFields(logrus.Fields{
			"component":     componentName,
			"signaled_for":  state,
			"current_state": currentGlobalState,
		}).Warn("Component signaled completion for a state we're not in")
		return nil // Don't decrement the wait group if we're not in that state
	}

	coordination.wg.Done()
	log.WithFields(logrus.Fields{
		"component": componentName,
		"state":     state,
	}).Info("Component signaled coordination complete")

	return nil
}

// GetComponentState returns the current state of a component
func GetComponentState(componentName string) (State, error) {
	mu.RLock()
	defer mu.RUnlock()

	component, exists := components[componentName]
	if !exists {
		return "", fmt.Errorf("component %s not registered", componentName)
	}

	return component.currentState, nil
}

// GetAllComponentStates returns a map of all components with their current states
func GetAllComponentStates() map[string]State {
	mu.RLock()
	defer mu.RUnlock()

	result := make(map[string]State, len(components))
	for name, comp := range components {
		result[name] = comp.currentState
	}

	return result
}

// ComponentsInState returns a list of components currently in the specified state
func ComponentsInState(state State) []string {
	mu.RLock()
	defer mu.RUnlock()

	var result []string
	for name, comp := range components {
		if comp.currentState == state {
			result = append(result, name)
		}
	}

	return result
}

// CurrentState returns the current global state of the system
// This is optimized for very fast concurrent access
func CurrentState() State {
	return globalState.Load().(State)
}

// IsTransitionAllowed checks if a transition from->to is allowed
func IsTransitionAllowed(from, to State) bool {

	allowed, exists := allowedTransitions[from]
	if !exists {
		return false
	}

	for _, state := range allowed {
		if state == to {
			return true
		}
	}

	return false
}

// WaitForState waits until the system reaches the specified state
// Returns a map with all components and their current states
// A nil map means the overall state wasn't reached (timeout)
func WaitForState(componentName string, state State, timeout time.Duration) map[string]State {
	log.WithFields(logrus.Fields{
		"component": componentName,
		"state":     state,
		"timeout":   timeout,
	}).Info("Waiting for state")

	// Fast path: check if we're already in the desired state
	if CurrentState() == state {
		return GetAllComponentStates()
	}

	// Set up timeout
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	} else {
		// Create a channel that will never receive a value (infinite timeout)
		neverCh := make(chan time.Time)
		timeoutCh = neverCh
	}

	// Channel to signal success
	doneCh := make(chan struct{})

	// Start a goroutine to wait for the state
	go func() {
		stateWaitersMu.Lock()
		for CurrentState() != state {
			stateWaiters[state].Wait()
		}
		stateWaitersMu.Unlock()
		close(doneCh)
	}()

	// Wait for either success or timeout
	select {
	case <-doneCh:
		log.WithFields(logrus.Fields{
			"component": componentName,
			"state":     state,
		}).Info("Finished waiting for state")
		return GetAllComponentStates()
	case <-timeoutCh:
		log.WithFields(logrus.Fields{
			"component": componentName,
			"state":     state,
		}).Warn("Timed out waiting for state")
		return nil
	}
}

// Change attempts to change the state of the system
// It returns a channel that will be closed when the state change is complete
func Change(to State, reason string) (chan struct{}, error) {
	// First check if we can acquire the transition lock
	if !transitionInProgress.CompareAndSwap(false, true) {
		log.Error("another transition already in progress")
		return nil, errors.New("another transition is already in progress")
	}

	mu.Lock()
	from := CurrentState()

	// Check if transition is allowed
	if !IsTransitionAllowed(from, to) {
		mu.Unlock()
		log.WithField("from", from).WithField("to", to).Error("Transition not allowed")
		return nil, fmt.Errorf("transition from %s to %s is not allowed", from, to)
	}

	log.WithField("from", from).WithField("to", to).Info("Changing state")

	// Create a channel to signal completion
	done := make(chan struct{})

	// Create transition record
	transition := Transition{
		From:      from,
		To:        to,
		Timestamp: time.Now(),
		Reason:    reason,
	}

	// Store the transition in history
	history = append(history, transition)

	// Reset coordination for this state
	coordination := stateCoordination[to]
	coordination.mu.Lock()

	// Determine which components need to be waited for (if not in monitor mode)
	componentsToCoordinate := []string{}

		for name, comp := range components {
			if comp.coordinatedStates[to] {
				componentsToCoordinate = append(componentsToCoordinate, name)
			}
		}


	log.WithField("components", componentsToCoordinate).Info("Components to coordinate with")

	// Clear completed state tracking map before adding to waitgroup
	for k := range coordination.completed {
		delete(coordination.completed, k)
	}

	// Reset the wait group (create a new one to avoid potential issues)
	// and add the components that need coordination
	coordination.wg = sync.WaitGroup{}
	coordination.wg.Add(len(componentsToCoordinate))
	coordination.mu.Unlock()

	// Reset ready signals for all components for this state
	for _, comp := range components {
		delete(comp.readySignaled, to)
	}

	// Gather callbacks to execute
	callbacksToExecute := make(map[string]CallbackFunc)
	generalCallbacks := make(map[string]CallbackFunc)

	for name, comp := range components {
		if callback, hasCallback := comp.stateCallbacks[to]; hasCallback {
			callbacksToExecute[name] = callback
		}

		if comp.allCallback != nil {
			generalCallbacks[name] = comp.allCallback
		}
	}

	// Log which components will be notified
	logComponents := append([]string{}, componentsToCoordinate...)
	for name := range callbacksToExecute {
		found := false
		for _, n := range logComponents {
			if n == name {
				found = true
				break
			}
		}
		if !found {
			logComponents = append(logComponents, name)
		}
	}

	log.WithFields(logrus.Fields{
		"from":       from,
		"to":         to,
		"reason":     reason,
		"components": logComponents,
	}).Info("State change")

	// Update the state atomically before running callbacks
	// This ensures components can check the new state right away
	globalState.Store(to)

	// Signal state waiters for the new state
	stateWaitersMu.Lock()
	stateWaiters[to].Broadcast()
	stateWaitersMu.Unlock()

	mu.Unlock()

	// Start a goroutine to handle the transition
	go func() {
		// Execute all callbacks
		var wg sync.WaitGroup

		// Specific callbacks first
		for name, callback := range callbacksToExecute {
			wg.Add(1)
			go func(compName string, cb CallbackFunc) {
				defer wg.Done()

				err := cb(from, to)
				log.WithField("component", compName).Info("Callback executed")
				if err != nil {
					log.WithFields(logrus.Fields{
						"component": compName,
						"error":     err,
						"from":      from,
						"to":        to,
					}).Error("Error in callback")
				}
			}(name, callback)
		}

		// Then general callbacks
		for name, callback := range generalCallbacks {
			wg.Add(1)
			go func(compName string, cb CallbackFunc) {
				defer wg.Done()

				err := cb(from, to)
				log.WithField("component", compName).Info("Executed general callback")
				if err != nil {
					log.WithFields(logrus.Fields{
						"component": compName,
						"error":     err,
						"from":      from,
						"to":        to,
					}).Error("Error in general callback")
				}
			}(name, callback)
		}

		// Wait for all callbacks to complete
		wg.Wait()
		log.WithFields(logrus.Fields{
			"state": to,
		}).Info("All callbacks completed")

		// Wait for coordination to complete with timeout
		if len(componentsToCoordinate) > 0 {
			coordDone := make(chan struct{})
			go func() {
				coordination.wg.Wait()
				close(coordDone)
			}()

			// Set a reasonable timeout - could be configurable
			timeout := 30 * time.Second

			select {
			case <-coordDone:
				log.WithFields(logrus.Fields{
					"state": to,
				}).Info("All components completed coordination")
			case <-time.After(timeout):
				// Log which components didn't complete
				coordination.mu.Lock()
				incomplete := []string{}
				for _, name := range componentsToCoordinate {
					if !coordination.completed[name] {
						incomplete = append(incomplete, name)
					}
				}
				coordination.mu.Unlock()

				log.WithFields(logrus.Fields{
					"state":      to,
					"incomplete": incomplete,
					"timeout":    timeout,
				}).Warn("Coordination timed out")
			}
		} else {
			log.WithFields(logrus.Fields{
				"state": to,
			}).Info("No components registered for coordination in state")
		}

		// Mark transition as complete
		transitionInProgress.Store(false)

		// Signal completion
		close(done)
	}()

	return done, nil
}

// ChangeAndWait changes the state and waits for all callbacks to complete
func ChangeAndWait(to State, reason string) error {
	done, err := Change(to, reason)
	if err != nil {
		return err
	}

	// Wait for state change to complete
	<-done
	return nil
}

// History returns a copy of the transition history
func History() []Transition {
	mu.RLock()
	defer mu.RUnlock()

	// Create a copy to avoid external modification
	historyCopy := make([]Transition, len(history))
	copy(historyCopy, history)

	return historyCopy
}

// ComponentsRegisteredForState returns a list of components registered for a state
func ComponentsRegisteredForState(state State) []string {
	mu.RLock()
	defer mu.RUnlock()

	result := make([]string, len(registeredForState[state]))
	copy(result, registeredForState[state])

	return result
}

// ComponentsParticipatingInCoordination returns a list of components that
// participate in coordination for a state
func ComponentsParticipatingInCoordination(state State) []string {
	mu.RLock()
	defer mu.RUnlock()

	var result []string
	for name, comp := range components {
		if comp.coordinatedStates[state] {
			result = append(result, name)
		}
	}

	return result
}

// Status returns a detailed status report about the state machine
func Status() map[string]interface{} {
	mu.RLock()
	defer mu.RUnlock()

	status := make(map[string]interface{})
	status["current_state"] = CurrentState()
	status["transition_in_progress"] = transitionInProgress.Load()
	status["components_count"] = len(components)

	// Component status info
	componentStatus := make(map[string]map[string]interface{})
	for name, comp := range components {
		compStatus := make(map[string]interface{})

		// Current state of the component
		compStatus["current_state"] = comp.currentState

		// States this component has callbacks for
		registeredStates := make([]string, 0)
		for state := range comp.stateCallbacks {
			registeredStates = append(registeredStates, string(state))
		}
		compStatus["registered_states"] = registeredStates

		// States this component coordinates with
		coordStates := make([]string, 0)
		for state, participates := range comp.coordinatedStates {
			if participates {
				coordStates = append(coordStates, string(state))
			}
		}
		compStatus["coordinated_states"] = coordStates

		// States this component has signaled ready for
		readyStates := make([]string, 0)
		for state, signaled := range comp.readySignaled {
			if signaled {
				readyStates = append(readyStates, string(state))
			}
		}
		compStatus["ready_signaled_states"] = readyStates

		// Add to overall status
		componentStatus[name] = compStatus
	}
	status["components"] = componentStatus

	// State registration info
	stateRegs := make(map[string][]string)
	for state, comps := range registeredForState {
		stateRegs[string(state)] = comps
	}
	status["state_registrations"] = stateRegs

	// Group components by state
	componentsByState := make(map[string][]string)
	for state := range stateCoordination {
		componentsByState[string(state)] = ComponentsInState(state)
	}
	status["components_by_state"] = componentsByState

	// Transition history
	status["history_count"] = len(history)
	if len(history) > 0 {
		status["last_transition"] = history[len(history)-1]
	}

	return status
}

// SetLogger allows setting a custom logrus logger instance
func SetLogger(logger *logrus.Logger) {
	log = logger
}
