package delta

import (
	"fmt"
	"log"
	"mvcc-go/engine"
	"mvcc-go/engine/delta/storage"
	"mvcc-go/lock"
)

type Tx struct {
	ID         int
	level      engine.IsolationLevel
	engine     *DeltaEngine
	lockedKeys map[string]struct{}
	txInfo     storage.TxInfo
}

func newTx(e *DeltaEngine, level engine.IsolationLevel) *Tx {
	return &Tx{
		ID:         e.lastTxID,
		level:      level,
		engine:     e,
		lockedKeys: make(map[string]struct{}),
		txInfo:     e.txInfo.Clone(),
	}
}

func (tx *Tx) Get(key string) (string, error) {
	log.Printf("Get %+v\n", tx)
	if tx.level == engine.ReadCommitted {
		tx.txInfo = tx.engine.txInfo.Clone()
	}

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

var _ engine.Engine = &DeltaEngine{}

type DeltaEngine struct {
	storage      *storage.DeltaStorage
	lockManager  *lock.Manager
	lastTxID     int
	lastCommitNo int
	txInfo       *storage.TxInfo
}

func NewDeltaEngine() *DeltaEngine {
	return &DeltaEngine{
		storage:     storage.NewDeltaStorage(),
		lockManager: lock.NewManager(),
		txInfo: &storage.TxInfo{
			ActiveTxIDs: make(map[int]struct{}),
			MinTxID:     1, // first txID
		},
	}
}

func (e *DeltaEngine) Begin(level engine.IsolationLevel) engine.Tx {
	e.lastTxID++
	e.txInfo.ActiveTxIDs[e.lastTxID] = struct{}{}

	return newTx(e, level)
}

func (e *DeltaEngine) commit(tx *Tx) {
	e.lastCommitNo++

	e.txInfo.Delete(tx.ID, e.lastCommitNo)
}
