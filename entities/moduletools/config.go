package moduletools

// ClassConfig is a helper type which is passed to the module to read it's
// per-class config. This is - among other places - used when vectorizing and
// when validation schema config
type ClassConfig interface {
	Class() map[string]interface{}
	Property(propName string) map[string]interface{}
}
