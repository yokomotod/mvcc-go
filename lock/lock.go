package lock

import (
	"errors"
	"sync"
	"time"
)

const Timeout = 100 * time.Millisecond

var ErrTimeout = errors.New("timeout")
var ErrNotLocked = errors.New("not locked")

type LockType string

const (
	sLock LockType = "s"
	xLock LockType = "x"
)

type Manager struct {
	locks map[string]map[int]LockType
	cond  sync.Cond
}

func NewManager() *Manager {
	return &Manager{
		locks: make(map[string]map[int]LockType),
		cond:  sync.Cond{L: &sync.Mutex{}},
	}
}

func (m *Manager) SLock(txID int, key string) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if _, ok := m.locks[key]; ok {
		if _, ok := m.locks[key][txID]; ok {
			return nil
		}
	}

	start := time.Now()

	for {
		if time.Since(start) > Timeout {
			return ErrTimeout
		}
		if !m.hasOtherXLock(txID, key) {
			break
		}

		m.wait(Timeout - time.Since(start))
	}

	if _, ok := m.locks[key]; !ok {
		m.locks[key] = make(map[int]LockType)
	}
	m.locks[key][txID] = sLock

	return nil
}

func (m *Manager) XLock(txID int, key string) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if _, ok := m.locks[key]; ok {
		if lockType, ok := m.locks[key][txID]; ok && lockType == xLock {
			return nil
		}
	}

	start := time.Now()

	for {
		if time.Since(start) > Timeout {
			return ErrTimeout
		}
		if !m.hasOtherAnyLock(txID, key) {
			break
		}

		m.wait(Timeout - time.Since(start))
	}

	if _, ok := m.locks[key]; !ok {
		m.locks[key] = make(map[int]LockType)
	}
	m.locks[key][txID] = xLock

	return nil
}

func (m *Manager) Unlock(txID int, key string) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if _, ok := m.locks[key]; !ok {
		return ErrNotLocked
	}
	if _, ok := m.locks[key][txID]; !ok {
		return ErrNotLocked
	}

	delete(m.locks[key], txID)

	if len(m.locks[key]) > 0 {
		return nil
	}

	delete(m.locks, key)
	m.cond.Broadcast()

	return nil
}

func (m *Manager) hasOtherXLock(txID int, key string) bool {
	if len(m.locks[key]) != 1 { // 0: no lock, 1+: must be slock
		return false
	}

	for owner, lockType := range m.locks[key] { // there must be only 1 loop
		if owner != txID && lockType == xLock {
			return true
		}
	}

	return false
}

func (m *Manager) hasOtherAnyLock(txID int, key string) bool {
	if len(m.locks[key]) == 0 {
		return false
	}

	if _, ok := m.locks[key][txID]; ok && len(m.locks[key]) == 1 {
		return false
	}

	return true
}

// cond.Wait() with timeout. must be called with m.cond.L locked.
func (m *Manager) wait(timeout time.Duration) {
	done := make(chan struct{})

	go func() {
		m.cond.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		m.cond.L.Lock()
	}
}
