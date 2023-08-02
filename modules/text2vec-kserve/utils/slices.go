package utils

func Contains[T comparable](s []T, e T) bool {
	for _, el := range s {
		if e == el {
			return true
		}
	}
	return false
}
