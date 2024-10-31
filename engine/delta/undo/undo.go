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
	commitNo int
	records  []*Record
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
			commitNo: 0,
			records:  make([]*Record, 0),
		}
	}

	u.logs[txID].records = append(u.logs[txID].records, record)

	return UndoLogPtr{
		txID:     txID,
		logIndex: len(u.logs[txID].records) - 1,
	}
}

func (u *UndoLogs) HasLogs(txID int) bool {
	_, ok := u.logs[txID]
	return ok
}

func (u *UndoLogs) Delete(txID int) {
	delete(u.logs, txID)
}

func (u *UndoLogs) SetCommitNo(txID, commitNo int) {
	if _, ok := u.logs[txID]; !ok {
		return
	}

	u.logs[txID].commitNo = commitNo
}

func (u *UndoLogs) GetCommitNo(txID int) int {
	if _, ok := u.logs[txID]; !ok {
		return 0
	}

	return u.logs[txID].commitNo
}

func (u *UndoLogs) Len() int {
	cnt := 0
	for _, log := range u.logs {
		cnt += len(log.records)
	}

	return cnt
}
