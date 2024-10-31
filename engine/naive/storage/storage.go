package storage

type Record struct {
	key   string
	value string
}

type NaiveStorage struct {
	records []Record // key must be unique
}

func NewNaiveStorage() *NaiveStorage {
	return &NaiveStorage{
		records: make([]Record, 0),
	}
}

func (s *NaiveStorage) Get(key string) (string, bool) {
	for _, r := range s.records {
		if r.key != key {
			continue
		}

		return r.value, true
	}

	return "", false
}

func (s *NaiveStorage) Set(key, value string) {
	for i, r := range s.records {
		if r.key == key {
			s.records[i].value = value
			return
		}
	}

	s.records = append(s.records, Record{
		key:   key,
		value: value,
	})
}
