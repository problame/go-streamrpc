package streamrpc

import (
	"sync/atomic"
	"runtime"
)

type spinlock struct {
	status int32 // 0 = unlocked, 1 = locked
}


func (l *spinlock) TryLock() bool {
	return atomic.CompareAndSwapInt32(&l.status, 0, 1)
}

func (l *spinlock) Unlock() {
	if !atomic.CompareAndSwapInt32(&l.status, 1, 0) {
		panic("invariant violation")
	}
}

type cas struct {
	spinlock spinlock
	value int32
}

func (c *cas) CompareAndSwap(old, new int32) (prev int32) {
	for !c.spinlock.TryLock() {
		runtime.Gosched()
	}
	defer c.spinlock.Unlock()
	prev = atomic.LoadInt32(&c.value)
	if prev == old {
		atomic.StoreInt32(&c.value, new)
	}
	return prev
}
