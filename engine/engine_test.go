package engine_test

import (
	"mvcc-go/engine"
	"mvcc-go/engine/locking"
	"mvcc-go/engine/naive"
	"sync"
	"testing"
	"time"
)

func TestEngine(t *testing.T) {
	cases := []struct {
		name   string
		engine engine.Engine
		want1  string
		want2  string
	}{
		{
			name:   "Naive",
			engine: naive.NewNaiveEngine(),
			want1:  "value1", // dirty read
			want2:  "value2", // dirty read
		},
		{
			name:   "Locking",
			engine: locking.NewLockingEngine(),
			want1:  "value2", // read committed
			want2:  "value2", // read committed
		},
	}

	for _, c := range cases {
		// tx1: set key=valueX
		// tx1: commit
		// tx1: set key=value0
		// tx1: commit
		// tx2: set key=value1
		// tx3: get key
		// tx2: set key=value2
		// tx2: commit
		// tx3: get key

		t.Run(c.name, func(t *testing.T) {
			tx1 := c.engine.Begin()
			t.Log(`tx1.Set("key", "valueX")`)
			err := tx1.Set("key", "valueX")
			if err != nil {
				t.Error(err)
				return
			}
			t.Log("tx1.Commit()")
			err = tx1.Commit()
			if err != nil {
				t.Fatal(err)
			}

			tx2 := c.engine.Begin()
			t.Log(`tx2.Set("key", "value0")`)
			err = tx2.Set("key", "value0")
			if err != nil {
				t.Fatal(err)
				return
			}
			t.Log("tx2.Commit()")
			err = tx2.Commit()
			if err != nil {
				t.Fatal(err)
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()

				tx3 := c.engine.Begin()

				t.Log(`tx3.Set("key", "value1")`)
				err := tx3.Set("key", "value1")
				if err != nil {
					t.Error(err)
					return
				}

				time.Sleep(20 * time.Millisecond)

				t.Log(`tx3.Set("key", "value2")`)
				err = tx3.Set("key", "value2")
				if err != nil {
					t.Error(err)
					return
				}

				t.Log("tx3.Commit()")
				err = tx3.Commit()
				if err != nil {
					t.Error(err)
					return
				}
			}()

			time.Sleep(10 * time.Millisecond)

			go func() {
				defer wg.Done()

				tx4 := c.engine.Begin()

				t.Log(`tx4.Get("key") start`)
				got, err := tx4.Get("key")
				t.Logf(`tx4.Get("key") got %q, err %v`, got, err)
				if err != nil {
					t.Error(err)
					return
				}

				if got != c.want1 {
					t.Errorf("expected %q, but got %q", c.want1, got)
				}

				time.Sleep(20 * time.Millisecond)

				t.Log(`tx4.Get("key") start`)
				got, err = tx4.Get("key")
				t.Logf(`tx4.Get("key") got %q, err %v`, got, err)
				if err != nil {
					t.Error(err)
					return
				}

				if got != c.want2 {
					t.Errorf("expected %q, but got %q", c.want2, got)
				}

				t.Log("tx4.Commit()")
				err = tx4.Commit()
				if err != nil {
					t.Error(err)
					return
				}
			}()

			wg.Wait()
		})
	}
}
