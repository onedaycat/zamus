package common

type SetList []string

func NewSetList(keys ...string) SetList {
	return keys
}

func NewSetListFromList(keys []string) SetList {
	return keys
}

func (s SetList) IsEmpty() bool {
	if len(s) == 0 {
		return true
	}

	return false
}

func (s SetList) Set(key string) {
	for i := range s {
		if s[i] == key {
			return
		}
	}

	s = append(s, key)
}

func (s SetList) Delete(key string) {
	for i := range s {
		if s[i] == key {
			s = s[:i+copy(s[i:], s[i+1:])]
		}
	}
}

func (s SetList) Has(key string) bool {
	for i := range s {
		if s[i] == key {
			return true
		}
	}

	return false
}

func (s SetList) List() []string {
	return s
}

func (s SetList) Clear() {
	s = s[:0]
}
