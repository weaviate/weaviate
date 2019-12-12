package search

// LocalRef to be filled by the search backend to indicate that the
// particular reference field is a local ref and does not require further
// resolving, as opposed to a NetworkRef.
type LocalRef struct {
	Class  string
	Fields map[string]interface{}
}
