package utils

import (
	"reflect"
	"sync"
)

type Slice interface {
	Apppend(item interface{})
	Get(int) interface{}
	Del(int)
	IsEmpty() bool
	Iter() <-chan SafeItem
	Size() int
	Remove(interface{})
	Contains(interface{}) bool
}

type SafeSlice struct {
	lock  *sync.Mutex
	items []interface{}
}
type SafeItem struct {
	Index int
	Value interface{}
}

func NewSafeSlice() Slice {
	lock := new(sync.Mutex)
	return &SafeSlice{
		lock:  lock,
		items: make([]interface{}, 0),
	}
}

func (s *SafeSlice) Apppend(item interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items = append(s.items, item)
}

func (s *SafeSlice) Del(index int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items = append(s.items[:index], s.items[index+1:]...)
}
func (s *SafeSlice) Get(index int) interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	if index < len(s.items) {
		return s.items[index]
	}
	return nil
}
func (s *SafeSlice) IsEmpty() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.items) == 0 {
		return true
	}
	return false
}
func (s *SafeSlice) Iter() <-chan SafeItem {
	ch := make(chan SafeItem, s.Size())
	go func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		for i, v := range s.items {
			ch <- SafeItem{i, v}
		}
		defer close(ch)
	}()

	return ch
}

func (s *SafeSlice) Size() int {
	return len(s.items)
}
func (s *SafeSlice) Remove(item interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, v := range s.items {
		if reflect.DeepEqual(item, v) {
			s.items = append(s.items[:i], s.items[i+1:]...)
			break
		}
	}
}
func (s *SafeSlice) Contains(item interface{}) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, v := range s.items {
		if reflect.DeepEqual(item, v) {
			return true
		}
	}
	return false
}
