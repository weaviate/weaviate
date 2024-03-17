package traverser

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/searchparams"
)

func (e *Explorer) validateEF(ef *searchparams.EF) error {
	if ef == nil {
		return nil
	}

	if ef.DynamicMax == 0 || ef.DynamicMin == 0 || ef.DynamicFactor == 0 {
		return fmt.Errorf("if specify ef, you need to specify three parameters at the same time")
	}

	return nil
}
