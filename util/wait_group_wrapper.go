package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

// 作用是异步的执行 cb
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
