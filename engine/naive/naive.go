package naive

import (
	"mvcc-go/engine"
	"mvcc-go/engine/naive/storage"
)

type naiveTx struct {
	storage *storage.NaiveStorage
}

func newTx(storage *storage.NaiveStorage) *naiveTx {
	return &naiveTx{
		storage: storage,
	}
}

func (tx *naiveTx) Get(key string) (string, error) {
	value, ok := tx.storage.Get(key)
	if !ok {
		return "", engine.ErrNotFound
	}

	return value, nil
}

func (tx *naiveTx) Set(key, value string) error {
	tx.storage.Set(key, value)

	return nil
}

func (tx *naiveTx) Commit() error {
	return nil
}

var _ engine.Engine = &NaiveEngine{}

type NaiveEngine struct {
	storage *storage.NaiveStorage
}

func NewNaiveEngine() *NaiveEngine {
	return &NaiveEngine{
		storage: storage.NewNaiveStorage(),
	}
}

func (e *NaiveEngine) Begin(level engine.IsolationLevel) engine.Tx {
	return newTx(e.storage)
}
