package inverted

import (
    "bytes"
    "context"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type rowOperation func([]byte, [][]byte) (bool, error)

func (rr *RowReader) likeHelper(ctx context.Context, like *likeRegexp, c lsmkv.CursorSet, rp rowOperation) error {
	var (
		initialK []byte
		initialV [][]byte
	)

	if like.optimizable {
		initialK, initialV = c.Seek(like.min)
	} else {
		initialK, initialV = c.First()
	}

	for k, v := initialK, initialV; k != nil; k, v = c.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		if like.optimizable {
			// if the query is optimizable, i.e. it doesn't start with a wildcard, we
			// can abort once we've moved past the point where the fixed characters
			// no longer match
			if len(k) < len(like.min) {
				break
			}

			if bytes.Compare(like.min, k[:len(like.min)]) == -1 {
				break
			}
		}

		if !like.regexp.Match(k) {
			continue
		}

		continueReading, err := rp(k, v)
		if err != nil {
			return err
		}

		if !continueReading {
			break
		}
	}

	return nil
}
