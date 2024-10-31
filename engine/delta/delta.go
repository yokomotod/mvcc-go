package delta

import (
	"fmt"
	"log"
	"mvcc-go/engine"
	"mvcc-go/engine/delta/storage"
	"mvcc-go/lock"
	"slices"
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
	purgeList    []int
}

func NewDeltaEngine() *DeltaEngine {
	return &DeltaEngine{
		storage:     storage.NewDeltaStorage(),
		lockManager: lock.NewManager(),
		txInfo: &storage.TxInfo{
			ActiveTxIDs:   make(map[int]struct{}),
			MinTxID:       1, // first txID
			LastCommitNos: make(map[int]int),
			MinCommitNo:   0,
		},
		purgeList: make([]int, 0),
	}
}

func (e *DeltaEngine) Begin(level engine.IsolationLevel) engine.Tx {
	e.lastTxID++
	e.txInfo.ActiveTxIDs[e.lastTxID] = struct{}{}
	e.txInfo.LastCommitNos[e.lastTxID] = e.lastCommitNo

	log.Printf("Begin tx%d, MinCommitNo=%d", e.lastTxID, e.txInfo.MinCommitNo)
	return newTx(e, level)
}

func (e *DeltaEngine) commit(tx *Tx) {
	e.lastCommitNo++

	e.storage.UndoLogs.SetCommitNo(tx.ID, e.lastCommitNo)

	e.txInfo.Delete(tx.ID, e.lastCommitNo)

	e.purge(tx)
}

func (e *DeltaEngine) purge(tx *Tx) {
	log.Printf("purge MinCommitNo=%d", tx.engine.txInfo.MinCommitNo)

	if tx.engine.storage.UndoLogs.HasLogs(tx.ID) {
		e.purgeList = append(e.purgeList, tx.ID)
	}

	log.Printf("purgeQueue=%+v", e.purgeList)
	for i := 0; i < len(e.purgeList); {
		txID := e.purgeList[i]
		log.Printf("purging txID=%d", txID)

		if tx.engine.storage.UndoLogs.GetCommitNo(txID) > tx.engine.txInfo.MinCommitNo {
			log.Printf("tx%d has no undoLogs to purge", txID)
			i++
			continue
		}

		log.Printf("tx%d has undoLogs to purge", txID)
		e.storage.UndoLogs.Delete(txID)

		// remove from purgeQueue
		e.purgeList = slices.Delete(e.purgeList, i, i+1)
	}
}

func (e *DeltaEngine) GC() (active, removed int) {
	return e.storage.UndoLogs.Len(), 0
}
