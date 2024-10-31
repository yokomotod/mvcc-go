package locking

import (
	"fmt"
	"mvcc-go/engine"
	"mvcc-go/engine/naive/storage"
	"mvcc-go/lock"
)

type Tx struct {
	ID         int
	engine     *LockingEngine
	lockedKeys map[string]struct{}
}

func newTx(engine *LockingEngine, id int) *Tx {
	return &Tx{
		ID:         id,
		engine:     engine,
		lockedKeys: make(map[string]struct{}),
	}
}

func (tx *Tx) Get(key string) (string, error) {
	err := tx.engine.lockManager.SLock(tx.ID, key)
	if err != nil {
		return "", fmt.Errorf("slock: %w", err)
	}

	tx.lockedKeys[key] = struct{}{}

	value, ok := tx.engine.storage.Get(key)
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

	tx.engine.storage.Set(key, value)

	return nil
}

func (tx *Tx) Commit() error {
	for key := range tx.lockedKeys {
		err := tx.engine.lockManager.Unlock(tx.ID, key)
		if err != nil {
			return fmt.Errorf("unlock: %w", err)
		}
	}

	return nil
}

var _ engine.Engine = &LockingEngine{}

type LockingEngine struct {
	storage     *storage.NaiveStorage
	lockManager *lock.Manager
	maxTxID     int
}

func NewLockingEngine() *LockingEngine {
	return &LockingEngine{
		storage:     storage.NewNaiveStorage(),
		lockManager: lock.NewManager(),
		maxTxID:     0,
	}
}

func (e *LockingEngine) Begin(level engine.IsolationLevel) engine.Tx {
	e.maxTxID++

	return newTx(e, e.maxTxID)
}

func (e *LockingEngine) GC() (active, removed int) {
	return 0, 0
}
