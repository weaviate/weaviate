package inverted

import (
	"bytes"
	"regexp"

	"github.com/pkg/errors"
)

type likeRegexp struct {
	// optimizable bool
	// min         []byte
	regexp *regexp.Regexp
}

func parseLikeRegexp(in []byte) (*likeRegexp, error) {
	r, err := regexp.Compile(transformLikeStringToRegexp(in))
	if err != nil {
		return nil, errors.Wrap(err, "compile regex from 'like' string")
	}

	return &likeRegexp{
		regexp: r,
	}, nil
}

func transformLikeStringToRegexp(in []byte) string {
	in = bytes.ReplaceAll(in, []byte("?"), []byte("."))
	in = bytes.ReplaceAll(in, []byte("*"), []byte(".*"))
	return "^" + string(in) + "$"
}
