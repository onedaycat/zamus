package common

type spy struct {
	counters map[string]int
}

func Spy() *spy {
	return &spy{
		counters: make(map[string]int),
	}
}

func (s *spy) Called(key string) {
	s.counters[key] += 1
}

func (s *spy) Count(key string) int {
	return s.counters[key]
}

func (s *spy) Has(key string) bool {
	_, ok := s.counters[key]
	return ok
}
