package helper

func NilThunk() func() interface{} {
	return func() interface{} {
		return nil
	}
}

func IdentityThunk(x interface{}) func() interface{} {
	return func() interface{} {
		return x
	}
}

func EmptyListThunk() func() interface{} {
	return func() interface{} {
		list := []interface{}{}
		return interface{}(list)
	}
}

func SingletonThunk(x interface{}) func() interface{} {
	return func() interface{} {
		return interface{}(x)
	}
}
