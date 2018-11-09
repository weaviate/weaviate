package helper

func nilThunk() func() interface{} {
	return func() interface{} {
		return nil
	}
}

func identityThunk(x interface{}) func() interface{} {
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
