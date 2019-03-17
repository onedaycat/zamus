package common

import (
	"sync"
)

type SpyTest struct {
	locker   sync.Mutex
	counters map[string]int
}

func Spy() *SpyTest {
	return &SpyTest{
		counters: make(map[string]int),
	}
}

func (s *SpyTest) Called(key string) {
	s.locker.Lock()
	s.counters[key] += 1
	s.locker.Unlock()
}

func (s *SpyTest) Count(key string) int {
	return s.counters[key]
}

func (s *SpyTest) Has(key string) bool {
	_, ok := s.counters[key]
	return ok
}

func (s *SpyTest) Reset() {
	s.counters = make(map[string]int)
}
