package inverted

import (
	"regexp"

	"github.com/pkg/errors"
)

type likeRegexp struct {
	// optimizable bool
	// min         []byte
	regexp *regexp.Regexp
}

func parseLikeRegexp(in []byte) (*likeRegexp, error) {
	r, err := regexp.Compile("^" + string(in) + "$")
	if err != nil {
		return nil, errors.Wrap(err, "compile regex from 'like' string")
	}

	return &likeRegexp{
		regexp: r,
	}, nil
}
