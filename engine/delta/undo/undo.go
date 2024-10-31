package undo

import "log"

type Record struct {
	Key   string
	Value string
	TxID  int
	Prev  *UndoLogPtr
}

type UndoLogPtr struct {
	txID     int
	logIndex int
}

type undoLog struct {
	records []*Record
}

type UndoLogs struct {
	logs map[int]*undoLog
}

func NewUndoLogs() *UndoLogs {
	return &UndoLogs{
		logs: make(map[int]*undoLog),
	}
}

func (u *UndoLogs) Get(ptr UndoLogPtr) *Record {
	// debug
	for txID, undoLog := range u.logs {
		log.Printf("tx%d: %+v", txID, undoLog)
	}

	log, ok := u.logs[ptr.txID]
	if !ok {
		return nil
	}

	return log.records[ptr.logIndex]
}

func (u *UndoLogs) Append(txID int, record *Record) UndoLogPtr {
	if _, ok := u.logs[txID]; !ok {
		u.logs[txID] = &undoLog{
			records: make([]*Record, 0),
		}
	}

	u.logs[txID].records = append(u.logs[txID].records, record)

	return UndoLogPtr{
		txID:     txID,
		logIndex: len(u.logs[txID].records) - 1,
	}
}
