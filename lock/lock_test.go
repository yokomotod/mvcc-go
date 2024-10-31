package lock_test

import (
	"errors"
	"log"
	"mvcc-go/lock"
	"testing"
	"time"
)

func TestXLockXLockByMySelf(t *testing.T) {
	manager := lock.NewManager()

	log.Println("XLock")
	err := manager.XLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("XLock by myself, should not be locked")
	err = manager.XLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestXLockSLockByMySelf(t *testing.T) {
	manager := lock.NewManager()

	log.Println("XLock")
	err := manager.XLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("SLock by myself, should not be locked")
	err = manager.SLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestSLockSLockByMySelf(t *testing.T) {
	manager := lock.NewManager()

	log.Println("SLock")
	err := manager.SLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("SLock by myself, should not be locked")
	err = manager.SLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnlockWithoutLock(t *testing.T) {
	manager := lock.NewManager()

	log.Println("Unlock without lock")
	err := manager.Unlock(1, "key")
	if !errors.Is(err, lock.ErrNotLocked) {
		t.Errorf("expected %v, but got %v", lock.ErrNotLocked, err)
	}
}

func TestXLock(t *testing.T) {
	manager := lock.NewManager()

	log.Println("XLock")
	err := manager.XLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("SLock, should locked and timeout")
	err = manager.SLock(2, "key")
	if !errors.Is(err, lock.ErrTimeout) {
		t.Errorf("expected %v, but got %v", lock.ErrTimeout, err)
	}

	log.Println("XLock, should locked and timeout")
	err = manager.XLock(2, "key")
	if !errors.Is(err, lock.ErrTimeout) {
		t.Errorf("expected %v, but got %v", lock.ErrTimeout, err)
	}

	log.Println("Unlock, there is no lock")
	err = manager.Unlock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("SLock, now should be able to lock")
	err = manager.SLock(2, "key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestSLock(t *testing.T) {
	// t.Parallel() // Parrallel makes log messages mixed up

	manager := lock.NewManager()

	log.Println("SLock")
	err := manager.SLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("SLock by other, should not be locked")
	err = manager.SLock(2, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("XLock, should be locked and timeout")
	err = manager.XLock(3, "key")
	if !errors.Is(err, lock.ErrTimeout) {
		t.Errorf("expected %v, but got %v", lock.ErrTimeout, err)
	}

	log.Println("Unlock, there are still 1 slock")
	err = manager.Unlock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("XLock, still should be locked and timeout")
	err = manager.XLock(3, "key")
	if !errors.Is(err, lock.ErrTimeout) {
		t.Errorf("expected %v, but got %v", lock.ErrTimeout, err)
	}

	log.Println("Unlock again, there is no lock")
	err = manager.Unlock(2, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("XLock, now should be able to lock")
	err = manager.XLock(3, "key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestSLockUpgrade(t *testing.T) {
	manager := lock.NewManager()

	log.Println("SLock")
	err := manager.SLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("upgrade to XLock")
	err = manager.XLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("SLock, should be locked and timeout")
	err = manager.SLock(2, "key")
	if !errors.Is(err, lock.ErrTimeout) {
		t.Errorf("expected %v, but got %v", lock.ErrTimeout, err)
	}
}

func TestSLockUpgradeWait(t *testing.T) {
	manager := lock.NewManager()

	log.Println("SLock")
	err := manager.SLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("SLock by other")
	err = manager.SLock(2, "key")
	if err != nil {
		t.Fatal(err)
	}

	log.Println("upgrade to XLock, should be locked and timeout")
	err = manager.XLock(1, "key")
	if !errors.Is(err, lock.ErrTimeout) {
		t.Errorf("expected %v, but got %v", lock.ErrTimeout, err)
	}
}

func TestManyWaits(t *testing.T) {
	// t.Parallel() // Parrallel makes log messages mixed up

	manager := lock.NewManager()

	log.Println("XLock")
	err := manager.XLock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	waiters := 10
	res := make([]error, waiters)
	for i := range waiters {
		res[i] = errors.New("no result")

		go func() {
			txID := i + 2
			err := manager.XLock(txID, "key")
			log.Printf("Xlock %d: %v\n", txID, err)
			res[i] = err
		}()
	}

	time.Sleep(lock.Timeout / 2)

	log.Println("Unlock, should 1 waiter succeed to lock")
	err = manager.Unlock(1, "key")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(lock.Timeout/2 + 10*time.Millisecond) // 10ms for margin

	success := 0
	timeout := 0

	for _, err := range res {
		if err == nil {
			success++
			continue
		}
		if errors.Is(err, lock.ErrTimeout) {
			timeout++
			continue
		}

		t.Errorf("unexpected error: %v", err)
	}

	if success != 1 {
		t.Errorf("expected 1 success, but got %d", success)
	}

	if timeout != waiters-1 {
		t.Errorf("expected %d timeouts, but got %d", waiters-1, timeout)
	}
}
