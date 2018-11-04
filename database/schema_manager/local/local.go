package local

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/creativesoftwarefdn/weaviate/database"
	db_schema "github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema_migrator"
	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
	log "github.com/sirupsen/logrus"
)

type LocalSchemaConfig struct {
	StateDir *string `json:"state_dir"`
}

type localSchemaManager struct {
	// The directory where the state will be stored
	stateDir string

	// Persist schema
	stateFile   *os.File
	schemaState localSchemaState

	// Persist connector specific state.
	connectorStateFile   *os.File
	connectorState       json.RawMessage
	connectorStateSetter connector_state.Connector

	// Calling the migrator
	connectorMigrator schema_migrator.Migrator
	callbacks         []func(updatedSchema db_schema.Schema)
}

// The state that will be serialized to/from disk.
// TODO: refactor to database/schema_manager, so that it can be re-used for distributed version.
type localSchemaState struct {
	ActionSchema *models.SemanticSchema `json:"action"`
	ThingSchema  *models.SemanticSchema `json:"thing"`
}

func (l *localSchemaState) SchemaFor(k kind.Kind) *models.SemanticSchema {
	switch k {
	case kind.THING_KIND:
		return l.ThingSchema
	case kind.ACTION_KIND:
		return l.ActionSchema
	default:
		log.Fatalf("Passed wrong neither thing nor kind, but %v", k)
		return nil
	}
}

func New(stateDirName string, connectorMigrator schema_migrator.Migrator) (database.SchemaManager, error) {
	manager := &localSchemaManager{
		stateDir:          stateDirName,
		schemaState:       localSchemaState{},
		connectorMigrator: connectorMigrator,
	}

	err := manager.loadOrInitializeSchema()
	if err != nil {
		return nil, err
	}

	err = manager.loadOrInitializeConnectorState()
	if err != nil {
		return nil, err
	}

	return manager, nil
}

// Load the state from a file, or if the files do not exist yet, initialize an empty schema.
func (l *localSchemaManager) loadOrInitializeSchema() error {
	// Verify that the directory exists and is indeed a directory.
	dirStat, err := os.Stat(l.stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("Directory '%v' does not exist", l.stateDir)
		} else {
			return fmt.Errorf("Could not look up stats of '%v'", l.stateDir)
		}
	} else {
		if !dirStat.IsDir() {
			return fmt.Errorf("The path '%v' is not a directory!", l.stateDir)
		}
	}

	// Check if the schema files exist.
	stateFileStat, err := os.Stat(l.statePath())
	var stateFileExists bool = true
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("Could not look up stats of '%v'", l.statePath())
		} else {
			stateFileExists = false
		}
	} else {
		if stateFileStat.IsDir() {
			return fmt.Errorf("The path '%v' is a directory, not a file!", l.statePath())
		}
	}

	// Open the file. If it's created, only allow the current user to access it.
	l.stateFile, err = os.OpenFile(l.statePath(), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("Could not open state file '%v'", l.statePath())
	}

	// Fill in empty ontologies if there is no schema file.
	if !stateFileExists {
		l.schemaState.ActionSchema = &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
			Type:    "action",
		}

		l.schemaState.ThingSchema = &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
			Type:    "thing",
		}

		// And flush the schema to disk.
		err := l.saveToDisk()
		if err != nil {
			log.Fatal("Could not save empty schema to disk")
		}
		log.Infof("Initialized empty schema")
	} else {
		// Load the state from disk.
		stateBytes, _ := ioutil.ReadAll(l.stateFile)
		var state localSchemaState
		err := json.Unmarshal(stateBytes, &state)
		if err != nil {
			return fmt.Errorf("Could not parse the schema state file '%v'", l.statePath())
		} else {
			l.schemaState = state
		}
	}
	return nil
}

// Load the
func (l *localSchemaManager) loadOrInitializeConnectorState() error {
	// Verify that the directory exists and is indeed a directory.
	dirStat, err := os.Stat(l.stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("Directory '%v' does not exist", l.stateDir)
		} else {
			return fmt.Errorf("Could not look up stats of '%v'", l.stateDir)
		}
	} else {
		if !dirStat.IsDir() {
			return fmt.Errorf("The path '%v' is not a directory!", l.stateDir)
		}
	}

	// Check if the schema files exist.
	stateFileStat, err := os.Stat(l.connectorStatePath())
	var stateFileExists bool = true
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("Could not look up stats of '%v'", l.connectorStatePath())
		} else {
			stateFileExists = false
		}
	} else {
		if stateFileStat.IsDir() {
			return fmt.Errorf("The path '%v' is a directory, not a file!", l.connectorStatePath())
		}
	}

	// Open the file. If it's created, only allow the current user to access it.
	l.connectorStateFile, err = os.OpenFile(l.connectorStatePath(), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("Could not open connector state file '%v'", l.connectorStatePath())
	}

	// Fill in null state
	if !stateFileExists {
		l.connectorState = json.RawMessage([]byte("null"))

		// And flush the connector state
		err := l.saveConnectorStateToDisk()
		if err != nil {
			log.Fatal("Could not save null connector state to disk")
		}
		log.Infof("Initialized empty schema")
	} else {
		// Load the state from disk.
		stateBytes, _ := ioutil.ReadAll(l.stateFile)
		err := l.connectorState.UnmarshalJSON(stateBytes)
		if err != nil {
			return fmt.Errorf("Could not parse connector state '%v'", l.connectorStatePath())
		}
	}
	return nil
}

func (l *localSchemaManager) statePath() string {
	return l.stateDir + "/schema_state.json"
}

func (l *localSchemaManager) connectorStatePath() string {
	return l.stateDir + "/connector_state.json"
}

// Save the schema to the local disk.
// Triggers callbacks to all interested observers.
func (l *localSchemaManager) saveToDisk() error {
	log.Info("Updating local schema on disk")
	// TODO not 100% robust against failures.
	// we don't check IO errors yet

	stateBytes, err := json.Marshal(l.schemaState)
	if err != nil {
		return err
	}

	err = l.stateFile.Truncate(0)
	if err != nil {
		return err
	}

	_, err = l.stateFile.Seek(0, 0)
	if err != nil {
		return err
	}

	l.stateFile.Write(stateBytes)
	l.stateFile.Sync()

	l.triggerCallbacks()

	return nil
}

// Save the connector state to disk.
// This local implementation has no side effects (like updating peer weaviate instances)
func (l *localSchemaManager) saveConnectorStateToDisk() error {
	log.Info("Saving connector state on disk")

	stateBytes, err := l.connectorState.MarshalJSON()
	if err != nil {
		return err
	}

	err = l.connectorStateFile.Truncate(0)
	if err != nil {
		return err
	}

	_, err = l.connectorStateFile.Seek(0, 0)
	if err != nil {
		return err
	}

	l.connectorStateFile.Write(stateBytes)
	l.connectorStateFile.Sync()

	return nil
}
