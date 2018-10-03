package local

import (
	"encoding/json"
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/models"
	"io/ioutil"
	"os"
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
type localSchemaState struct {
	actionSchema *models.SemanticSchema `json:"action"`
	thingSchema  *models.SemanticSchema `json:"thing"`
}

func New(stateDirName string) (*LocalSchemaManager, error) {
	manager := &LocalSchemaManager{
		stateDir:    stateDirName,
		schemaState: localSchemaState{},
	}

	manager.load()
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
		if os.IsNotExist(err) {
			stateFileExists = false
		} else {
			return fmt.Errorf("Could not look up stats of '%v'", l.statePath())
		}
	} else {
		if stateFileStat.IsDir() {
			return fmt.Errorf("The path '%v' is a directory, not a file!", l.statePath())
		}
	}

	// Open the file. If it's created, only allow the current user to access it.
	l.stateFile, err = os.OpenFile(l.statePath(), os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("Could not open state file '%v'", l.statePath())
	}

	// Fill in empty ontologies if there is no schema file.
	if !stateFileExists {
		l.schemaState.actionSchema = &models.SemanticSchema{
			Type: "action",
		}

		l.schemaState.thingSchema = &models.SemanticSchema{
			Type: "thing",
		}

		// And flush the schema to disk.
		l.saveToDisk()
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
	// TODO not 100% robust against failures.
	// we don't check IO errors yet

	stateBytes, err := json.Marshal(l.schemaState)
	if err != nil {
		return err
	}

	l.stateFile.Truncate(0)
	l.stateFile.Seek(0, 0)
	l.stateFile.Write(stateBytes)

	return nil
}
