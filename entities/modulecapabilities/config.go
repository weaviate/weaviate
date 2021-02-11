package modulecapabilities

type ClassConfig interface {
	Class() map[string]interface{}
	Property(propName string) map[string]interface{}
}
