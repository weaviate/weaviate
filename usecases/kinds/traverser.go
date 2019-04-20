package kinds

// Traverser can be used to dynamically traverse the knowledge graph
type Traverser struct {
	locks locks
	repo  traverserRepo
}

type traverserRepo interface {
	LocalGetClass(LocalGetParams) (interface{}, error)
	LocalGetMeta(GetMetaParams) (interface{}, error)
}
