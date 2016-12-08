package concurrent

import (
	"sync"
)

type CounterType int64

type Counter struct {
	value CounterType
	lock  *sync.Mutex
}

func NewCounter() *Counter {
	return &Counter{lock: &sync.Mutex{}}
}

func (vSelf *Counter) IncreaseBy(pIncreaseBy CounterType) CounterType {
	vSelf.lock.Lock()
	defer vSelf.lock.Unlock()
	vSelf.value = vSelf.value + pIncreaseBy
	return vSelf.value
}

func (vSelf *Counter) GetValue() CounterType {
	return vSelf.IncreaseBy(0)
}
