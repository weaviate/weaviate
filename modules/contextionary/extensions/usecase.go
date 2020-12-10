package extensions

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/modules"
)

// UseCase handles all business logic regarding extensions
type UseCase struct {
	storage modules.Storage
}

func NewUseCase(storage modules.Storage) *UseCase {
	return &UseCase{
		storage: storage,
	}
}

func (uc *UseCase) Store(concept string, value []byte) error {
	err := uc.storage.Put([]byte(concept), value)
	if err != nil {
		return errors.Wrapf(err, "store concept %q", concept)
	}

	return nil
}

func (uc *UseCase) Load(concept string) ([]byte, error) {
	val, err := uc.storage.Get([]byte(concept))
	if err != nil {
		return nil, errors.Wrapf(err, "load concept %q", concept)
	}

	return val, nil
}

func (uc *UseCase) LoadAll() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := uc.storage.Scan(func(k, v []byte) (bool, error) {
		_, err := buf.Write(v)
		if err != nil {
			return false, errors.Wrapf(err, "write concept %q", string(k))
		}

		_, err = buf.Write([]byte("\n"))
		if err != nil {
			return false, errors.Wrapf(err, "write newline separator")
		}

		return true, nil
	})
	if err != nil {
		return nil, errors.Wrapf(err, "load all concepts")
	}

	return buf.Bytes(), nil
}
