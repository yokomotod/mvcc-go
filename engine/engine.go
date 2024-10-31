package engine

import "fmt"

var ErrNotFound = fmt.Errorf("not found")

type Tx interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Commit() error
}
type IsolationLevel string

const (
	ReadCommitted  IsolationLevel = "read_committed"
	RepeatableRead IsolationLevel = "repeatable_read"
)

type Engine interface {
	Begin(level IsolationLevel) Tx
}
