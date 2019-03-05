package common

type SpyTest struct {
	counters map[string]int
}

func Spy() *SpyTest {
	return &SpyTest{
		counters: make(map[string]int),
	}
}

func (s *SpyTest) Called(key string) {
	s.counters[key] += 1
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
