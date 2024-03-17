package common

type SearchWithEFOptions struct {
	DynamicMin    int
	DynamicMax    int
	DynamicFactor int
}

type SearchWithEFOption func(*SearchWithEFOptions)

func WithEFOptions(dynamicMin, dynamicMax, dynamicFactor int) SearchWithEFOption {
	return func(opts *SearchWithEFOptions) {
		opts.DynamicMin = dynamicMin
		opts.DynamicMax = dynamicMax
		opts.DynamicFactor = dynamicFactor
	}
}
