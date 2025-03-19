package runtime

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDemo(t *testing.T) {
	// Create dynamic flag manager.
	m := NewManager()

	// Initialize dynamic flags by specifying their path in the runtime config file
	// and what default value to use in case the path is not found or parsing is failing
	// You would usually use value from ENV variables as the default value.
	boolFlag := m.GetDynamicBool("my_bool", false)
	intFlag := m.GetDynamicInt("my_int", 10)
	stringFlag := m.GetDynamicString("my_string", "hello")

	// Initially all flags return default values.
	require.Equal(t, false, boolFlag.Get())
	require.Equal(t, 10, intFlag.Get())
	require.Equal(t, "hello", stringFlag.Get())

	// Simulate manager reloading the config file from disk.
	err := m.reload(`
my_bool: true
my_int: 20
my_string: world
`)
	require.NoError(t, err)

	// All flags now return values from the config file.
	require.Equal(t, true, boolFlag.Get())
	require.Equal(t, 20, intFlag.Get())
	require.Equal(t, "world", stringFlag.Get())

	// Config file is changed to override only subset of flags.
	err = m.reload(`
my_bool: true
`)
	require.NoError(t, err)

	// Flags that did not have overriding value in the config file are now back to returning default value.
	require.Equal(t, true, boolFlag.Get())
	require.Equal(t, 10, intFlag.Get())
	require.Equal(t, "hello", stringFlag.Get())
}

type DynamicConfigType interface {
	string | bool | int | float64
}

type DynamicFlag[T DynamicConfigType] struct {
	val          *atomic.Value
	defaultValue T
}

func NewDynamicFlag[T DynamicConfigType](defaultValue T) DynamicFlag[T] {
	a := &atomic.Value{}
	a.Store(defaultValue)

	return DynamicFlag[T]{
		val:          a,
		defaultValue: defaultValue,
	}
}

func (d DynamicFlag[T]) Get() T {
	v := d.val.Load()
	if v == nil {
		var zero T
		return zero
	}

	return v.(T)
}

// TODO: we can add something similar for YAML parsing as well
func (d DynamicFlag[T]) MarshalJSON() ([]byte, error) {
	if d.val == nil {
		return nil, nil
	}

	v := d.Get()
	return json.Marshal(v)
}

func (d *DynamicFlag[T]) UnmarshalJSON(data []byte) error {
	var t T
	if err := json.Unmarshal(data, &t); err != nil {
		return err
	}
	if d.val == nil {
		d.val = &atomic.Value{}
	}
	d.val.Store(t)
	return nil
}

func (d DynamicFlag[T]) update(val any) {
	if v, ok := val.(T); ok {
		d.val.Store(v)
	}
	// TODO: add logging that wrong type was passed
}

func (d DynamicFlag[T]) setDefault() {
	d.val.Store(d.defaultValue)
}

type updatableFlag interface {
	update(t any)
	setDefault()
}

// TODO: add synchronization
type Manager struct {
	registeredFlags map[string]updatableFlag
}

func NewManager() *Manager {
	return &Manager{
		registeredFlags: make(map[string]updatableFlag),
	}
}

func (m *Manager) GetDynamicBool(path string, defaultValue bool) DynamicFlag[bool] {
	flag := NewDynamicFlag[bool](defaultValue)
	m.registeredFlags[path] = flag
	return flag
}

func (m *Manager) GetDynamicString(path string, defaultValue string) DynamicFlag[string] {
	flag := NewDynamicFlag[string](defaultValue)
	m.registeredFlags[path] = flag
	return flag
}

func (m *Manager) GetDynamicInt(path string, defaultValue int) DynamicFlag[int] {
	flag := NewDynamicFlag[int](defaultValue)
	m.registeredFlags[path] = flag
	return flag
}

func (m *Manager) reload(data string) error {
	var fields map[string]any
	if err := yaml.Unmarshal([]byte(data), &fields); err != nil {
		// TODO: better error handling
		return err
	}

	for path, flag := range m.registeredFlags {
		field, ok := fields[path]
		if !ok {
			flag.setDefault()
			continue
		}
		flag.update(field)
	}
	return nil
}
