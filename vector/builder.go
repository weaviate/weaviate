// Interface that implements a builder to a
type VectorIndexBuilder interface {
  AddWord(string, Vector) error
  Build(k int) VectorInterface
}

func Builder() *VectorIndexBuilder {
  return nil
}
