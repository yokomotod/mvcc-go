package appendonly

import (
	"fmt"
	"mvcc-go/engine"
	"mvcc-go/engine/appendonly/storage"
	"mvcc-go/lock"
)

type Tx struct {
	ID         int
	engine     *AppendOnlyEngine
	lockedKeys map[string]struct{}
	txInfo     storage.TxInfo
}

func newTx(e *AppendOnlyEngine, txID int) *Tx {
	return &Tx{
		ID:         txID,
		engine:     e,
		lockedKeys: make(map[string]struct{}),
		txInfo:     e.txInfo.Clone(),
	}
}

func (tx *Tx) Get(key string) (string, error) {
	value, ok := tx.engine.storage.Get(key, tx.ID, tx.txInfo)
	if !ok {
		return "", engine.ErrNotFound
	}

	return value, nil
}

func (tx *Tx) Set(key, value string) error {
	err := tx.engine.lockManager.XLock(tx.ID, key)
	if err != nil {
		return fmt.Errorf("xlock: %w", err)
	}

	tx.lockedKeys[key] = struct{}{}

	tx.engine.storage.Set(key, value, tx.ID)

	return nil
}

func (tx *Tx) Commit() error {
	for key := range tx.lockedKeys {
		err := tx.engine.lockManager.Unlock(tx.ID, key)
		if err != nil {
			return fmt.Errorf("unlock: %w", err)
		}
	}

	tx.engine.commit(tx)

	return nil
}

var _ engine.Engine = &AppendOnlyEngine{}

type AppendOnlyEngine struct {
	storage     *storage.AppendOnlyStorage
	lockManager *lock.Manager
	maxTxID     int
	txInfo      *storage.TxInfo
}

func NewAppendOnlyEngine() *AppendOnlyEngine {
	return &AppendOnlyEngine{
		storage:     storage.NewAppendOnlyStorage(),
		lockManager: lock.NewManager(),
		maxTxID:     0,
		txInfo: &storage.TxInfo{
			ActiveTxIDs: make(map[int]struct{}),
			MinTxID:     1, // first txID
		},
	}
}

func (e *AppendOnlyEngine) Begin() engine.Tx {
	e.maxTxID++
	e.txInfo.ActiveTxIDs[e.maxTxID] = struct{}{}

	return newTx(e, e.maxTxID)
}

func (e *AppendOnlyEngine) commit(tx *Tx) {
	e.txInfo.Delete(tx.ID)
}
