package engine

import "fmt"

var ErrNotFound = fmt.Errorf("not found")

type Tx interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Commit() error
}

type Engine interface {
	Begin() Tx
}
