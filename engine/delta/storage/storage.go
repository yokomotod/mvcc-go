package storage

import (
	"log"
	"maps"
	"mvcc-go/engine/delta/undo"
	"slices"
)

type TxInfo struct {
	ActiveTxIDs map[int]struct{}
	MinTxID     int

	LastCommitNos map[int]int
	MinCommitNo   int
}

func (info *TxInfo) Clone() TxInfo {
	return TxInfo{
		ActiveTxIDs:   maps.Clone(info.ActiveTxIDs),
		MinTxID:       info.MinTxID,
		LastCommitNos: maps.Clone(info.LastCommitNos),
		MinCommitNo:   info.MinCommitNo,
	}
}

func (info *TxInfo) Delete(txID, lastCommitNo int) {
	delete(info.ActiveTxIDs, txID)
	delete(info.LastCommitNos, txID)

	if len(info.ActiveTxIDs) == 0 {
		info.MinTxID = 0
		info.MinCommitNo = lastCommitNo
		return
	}

	info.MinTxID = slices.Min(slices.Collect(maps.Keys(info.ActiveTxIDs)))
	info.MinCommitNo = slices.Min(slices.Collect(maps.Values(info.LastCommitNos)))
}

func isVisiable(recordTxID, myTxID int, txInfo TxInfo) bool {
	if recordTxID == myTxID {
		// 自分自身が書いたものは見える
		return true
	}

	if recordTxID > myTxID {
		// 自分より後のトランザクションが書いたものは見ない
		log.Printf("not visible because not committed")
		return false
	}

	if _, ok := txInfo.ActiveTxIDs[recordTxID]; ok {
		// アクティブなトランザクションが書いたものは見ない
		log.Printf("not visible because active")
		return false
	}

	return true
}

type DeltaStorage struct {
	records  []*undo.Record
	UndoLogs *undo.UndoLogs
}

func NewDeltaStorage() *DeltaStorage {
	return &DeltaStorage{
		records:  make([]*undo.Record, 0),
		UndoLogs: undo.NewUndoLogs(),
	}
}

func (s *DeltaStorage) Set(key, value string, txID int) {
	record := undo.Record{
		Key:   key,
		Value: value,
		TxID:  txID,
	}

	for i, r := range s.records {
		if r.Key != key {
			continue
		}

		if r.TxID == txID {
			// 自分が書いたレコードは直接更新して終了
			log.Printf("update latest myself")
			r.Value = value
			return
		}

		// 更新
		prevPtr := s.UndoLogs.Append(txID, s.records[i]) // 直前の値をundo logに追加
		log.Printf("update latest, new new undoPtr %d with %+v", prevPtr, *s.records[i])

		record.Prev = &prevPtr // undo logへのポインタを設定
		s.records[i] = &record // テーブルの値を更新

		log.Printf("update %v", record)
		return
	}

	// 新規追加
	ptr := s.UndoLogs.Append(txID, nil)
	record.Prev = &ptr
	s.records = append(s.records, &record)

	log.Printf("insert %v", record)
}

func (s *DeltaStorage) Get(key string, txID int, txInfo TxInfo) (string, bool) {
	var record *undo.Record
	for _, r := range s.records {
		if r.Key == key {
			record = r

			break // keyは一意なので見つかったら終了
		}
	}

	// 見えてよいバージョンまでundo logを辿る
	for {
		if record == nil {
			log.Println("not found")
			return "", false
		}

		if isVisiable(record.TxID, txID, txInfo) {
			return record.Value, true
		}

		record = s.UndoLogs.Get(*record.Prev)
		log.Printf("next prevPtr=%+v, prevRecord=%v", record.Prev, record)
	}
}
