package common

type Set map[string]struct{}

func NewSet() Set {
	return make(map[string]struct{}, 100)
}

func (s Set) Set(key string) {
	s[key] = struct{}{}
}

func (s Set) Has(key string) bool {
	_, ok := s[key]
	return ok
}

func (s Set) List() []string {
	i := 0
	keys := make([]string, len(s))
	for key := range s {
		keys[i] = key
		i++
	}

	return keys
}
