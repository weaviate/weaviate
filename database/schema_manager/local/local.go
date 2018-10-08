package local

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
	log "github.com/sirupsen/logrus"
)

type LocalSchemaConfig struct {
	StateDir *string `json:"state_dir"`
}

type LocalSchemaManager struct {
	// The directory where the state will be stored
	stateDir string

	// A file handler to the state file.
	stateFile   *os.File
	schemaState localSchemaState
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
		log.Fatal("Passed wrong neither thing nor kind, but %v", k)
		return nil
	}
}

func New(stateDirName string) (*LocalSchemaManager, error) {
	manager := &LocalSchemaManager{
		stateDir:    stateDirName,
		schemaState: localSchemaState{},
	}

	err := manager.load()
	if err != nil {
		return nil, err
	}

	return manager, nil
}

// Load the state from a file, or if the files do not exist yet, initialize an empty schema.
func (l *LocalSchemaManager) load() error {
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
			Type: "action",
		}

		l.schemaState.ThingSchema = &models.SemanticSchema{
			Type: "thing",
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

func (l *LocalSchemaManager) statePath() string {
	return l.stateDir + "/state.json"
}

// Save the schema to the local disk.
func (l *LocalSchemaManager) saveToDisk() error {
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

	return nil
}
