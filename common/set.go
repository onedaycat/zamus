package common

type Set map[string]struct{}

func NewSet(keys ...string) Set {
    s := make(map[string]struct{}, 100)
    if len(keys) > 0 {
        for _, key := range keys {
            s[key] = struct{}{}
        }
    }

    return s
}

func NewSetFromList(keys []string) Set {
    s := make(map[string]struct{}, 100)
    if len(keys) > 0 {
        for _, key := range keys {
            s[key] = struct{}{}
        }
    }

    return s
}

func (s Set) IsEmpty() bool {
    if len(s) == 0 {
        return true
    }

    return false
}

func (s Set) Set(key string) {
    s[key] = struct{}{}
}

func (s Set) SetMany(keys []string) {
    for _, key := range keys {
        if !s.Has(key) {
            s[key] = struct{}{}
        }
    }
}

func (s Set) Delete(key string) {
    delete(s, key)
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

func (s Set) Clear() {
    //noinspection GoAssignmentToReceiver
    s = make(map[string]struct{})
}
