package statemachine

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the possible states of the system
type State string

// System states
const (
	StateStartup     State = "startup"
	StateRunning     State = "running"
	StateBackup      State = "backup"
	StatePreShutdown State = "pre-shutdown"
	StateShutdown    State = "shutdown"
)

// Transition represents a state transition with callbacks
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
	name            string
	stateCallbacks  map[State]CallbackFunc
	allCallback     CallbackFunc
	coordinatedStates map[State]bool // States where component participates in coordination
}

// Module-level variables
var (
	currentState        atomic.Value
	mu                  sync.RWMutex
	history             []Transition
	components          = make(map[string]*componentInfo)
	allowedTransitions  = make(map[State][]State)
	registeredForState  = make(map[State][]string)      // Tracks which components are registered for each state
	stateCoordination   = make(map[State]*coordination) // Coordination info for each state
	transitionInProgress atomic.Bool
	initialized         atomic.Bool
	logger              = log.New(log.Writer(), "[StateMachine] ", log.LstdFlags)
)

// coordination tracks the coordination for a specific state
type coordination struct {
	wg        sync.WaitGroup
	mu        sync.Mutex
	completed map[string]bool
}

func init() {
	// Initialize with startup state
	currentState.Store(StateStartup)
	
	// Define allowed state transitions
	allowedTransitions[StateStartup] = []State{StateRunning}
	allowedTransitions[StateRunning] = []State{StateBackup, StatePreShutdown}
	allowedTransitions[StateBackup] = []State{StateRunning, StatePreShutdown}
	allowedTransitions[StatePreShutdown] = []State{StateShutdown}
	allowedTransitions[StateShutdown] = []State{} // No transitions from shutdown
	
	// Initialize coordination for each state
	allStates := []State{StateStartup, StateRunning, StateBackup, StatePreShutdown, StateShutdown}
	for _, state := range allStates {
		stateCoordination[state] = &coordination{
			completed: make(map[string]bool),
		}
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
		name:             name,
		stateCallbacks:   make(map[State]CallbackFunc),
		coordinatedStates: make(map[State]bool),
	}
	
	components[name] = component
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
	
	// Mark as completed and decrement wait group
	coordination := stateCoordination[state]
	coordination.mu.Lock()
	defer coordination.mu.Unlock()
	
	if !coordination.completed[componentName] {
		coordination.completed[componentName] = true
		coordination.wg.Done()
		logger.Printf("Component %s signaled coordination complete for state %s", componentName, state)
	}
	
	return nil
}

// CurrentState returns the current state of the system
// This is optimized for very fast concurrent access
func CurrentState() State {
	return currentState.Load().(State)
}

// IsTransitionAllowed checks if a transition from->to is allowed
func IsTransitionAllowed(from, to State) bool {
	mu.RLock()
	defer mu.RUnlock()
	
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

// Transition attempts to change the state of the system
// It returns a channel that will be closed when the transition is complete
func Transition(to State, reason string) (chan struct{}, error) {
	// First check if we can acquire the transition lock
	if !transitionInProgress.CompareAndSwap(false, true) {
		return nil, errors.New("another transition is already in progress")
	}
	
	mu.Lock()
	from := CurrentState()
	
	// Check if transition is allowed
	if !IsTransitionAllowed(from, to) {
		transitionInProgress.Store(false)
		mu.Unlock()
		return nil, fmt.Errorf("transition from %s to %s is not allowed", from, to)
	}
	
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
	for k := range coordination.completed {
		delete(coordination.completed, k)
	}
	coordination.mu.Unlock()
	
	// Determine which components need to be waited for
	componentsToCoordinate := []string{}
	for name, comp := range components {
		if comp.coordinatedStates[to] {
			componentsToCoordinate = append(componentsToCoordinate, name)
		}
	}
	
	// Set up wait group
	coordination.wg.Add(len(componentsToCoordinate))
	
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
	
	logger.Printf("State transition: %s -> %s (%s). Notifying components: %v", 
		from, to, reason, logComponents)
	
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
				if err != nil {
					logger.Printf("Error in callback for component %s: %v", compName, err)
				}
			}(name, callback)
		}
		
		// Then general callbacks
		for name, callback := range generalCallbacks {
			wg.Add(1)
			go func(compName string, cb CallbackFunc) {
				defer wg.Done()
				
				err := cb(from, to)
				if err != nil {
					logger.Printf("Error in general callback for component %s: %v", compName, err)
				}
			}(name, callback)
		}
		
		// Wait for all callbacks to complete
		wg.Wait()
		
		// Wait for coordination to complete with timeout
		coordDone := make(chan struct{})
		go func() {
			coordination.wg.Wait()
			close(coordDone)
		}()
		
		// Set a reasonable timeout - could be configurable
		timeout := 30 * time.Second
		
		select {
		case <-coordDone:
			logger.Printf("All components completed coordination for state %s", to)
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
			
			logger.Printf("WARNING: Coordination timed out for state %s. Components that didn't complete: %v", 
				to, incomplete)
		}
		
		// Update the state atomically
		currentState.Store(to)
		
		// Mark transition as complete
		transitionInProgress.Store(false)
		
		// Signal completion
		close(done)
	}()
	
	return done, nil
}

// TransitionAndWait changes the state and waits for all callbacks to complete
func TransitionAndWait(to State, reason string) error {
	done, err := Transition(to, reason)
	if err != nil {
		return err
	}
	
	// Wait for transition to complete
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
	
	// Component registration info
	componentStatus := make(map[string]map[string]interface{})
	for name, comp := range components {
		compStatus := make(map[string]interface{})
		
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
	
	// Transition history
	status["history_count"] = len(history)
	if len(history) > 0 {
		status["last_transition"] = history[len(history)-1]
	}
	
	return status
}

// SetLogger allows changing the logger used by the state machine
func SetLogger(newLogger *log.Logger) {
	logger = newLogger
}
