package db

// VectorIndex is anything that indexes vectors effieciently. For an example
// look at ./vector/hsnw/index.go
type VectorIndex interface {
	Add(id int, vector []float32) error // TODO: make id uint32
	SearchByID(id int, k int) ([]int, error)
	SearchByVector(vector []float32, k int) ([]int, error)
}
