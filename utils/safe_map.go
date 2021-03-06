package utils

import "sync"

type SafeMap struct {
	lock  *sync.Mutex
	items map[string]interface{}
}

type SafeMapItem struct {
	Key   string
	Value interface{}
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		lock:  &sync.Mutex{},
		items: make(map[string]interface{}, 0),
	}
}

func (sm *SafeMap) Set(key string, value interface{}) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	sm.items[key] = value
}

func (sm *SafeMap) Get(key string) (interface{}, bool) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	v, ok := sm.items[key]
	return v, ok
}

func (sm *SafeMap) Del(key string) {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	delete(sm.items, key)
}
func (sm *SafeMap) Contains(key string) bool {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	_, ok := sm.items[key]
	return ok
}
func (sm *SafeMap) Iter() <-chan SafeMapItem {
	ch := make(chan SafeMapItem)
	go func() {
		for k, v := range sm.items {
			ch <- SafeMapItem{k, v}
		}
	}()
	return ch
}
func (sm *SafeMap) IsEmpty() bool {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	size := len(sm.items)
	return size == 0
}
