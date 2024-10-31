package storage

import (
	"log"
	"maps"
	"slices"
)

type Record struct {
	Key       string
	Value     string
	BeginTxID int
	EndTxID   int
}

type TxInfo struct {
	ActiveTxIDs map[int]struct{}
	MinTxID     int
}

func (info *TxInfo) Clone() TxInfo {
	return TxInfo{
		ActiveTxIDs: maps.Clone(info.ActiveTxIDs),
		MinTxID:     info.MinTxID,
	}
}

func (info *TxInfo) Delete(txID int) {
	delete(info.ActiveTxIDs, txID)

	if len(info.ActiveTxIDs) == 0 {
		info.MinTxID = 0
		return
	}

	info.MinTxID = slices.Min(slices.Collect(maps.Keys(info.ActiveTxIDs)))
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

type AppendOnlyStorage struct {
	records []Record
}

func NewAppendOnlyStorage() *AppendOnlyStorage {
	return &AppendOnlyStorage{
		records: make([]Record, 0),
	}
}

func (s *AppendOnlyStorage) Get(key string, txID int, txInfo TxInfo) (string, bool) {
	var value string
	found := false
	for _, r := range s.records {
		if r.Key != key {
			continue
		}

		if !isVisiable(r.BeginTxID, txID, txInfo) {
			continue
		}

		value = r.Value
		found = true
	}

	return value, found
}

func (s *AppendOnlyStorage) Set(key, value string, txID int) {
	for i, r := range s.records {
		if r.Key != key {
			continue
		}

		if r.BeginTxID == txID {
			// update latest myself
			s.records[i].Value = value
			return
		}

		if r.EndTxID == 0 {
			s.records[i].EndTxID = txID
			break
		}
	}

	s.records = append(s.records, Record{
		Key:       key,
		Value:     value,
		BeginTxID: txID,
	})
}

func (s *AppendOnlyStorage) Vacuum(txInfo *TxInfo) (active, removed int) {
	s.records = slices.DeleteFunc(s.records, func(r Record) bool {
		if r.EndTxID == 0 {
			return false
		}

		if _, ok := txInfo.ActiveTxIDs[r.EndTxID]; ok {
			active++
			return false
		}

		log.Printf("remove %+v", r)
		removed++
		return true
	})

	return active, removed
}
