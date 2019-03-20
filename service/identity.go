package service

import (
	"strings"
)

type Identity struct {
	ID     string            `json:"id"`
	Email  string            `json:"email"`
	IPs    []string          `json:"ips"`
	Groups []string          `json:"groups"`
	Pems   map[string]string `json:"pems"`
}

func (id *Identity) HasPermission(key, permission string) bool {
	if id.Pems == nil {
		return false
	}

	ws, ok := id.Pems[key]
	if !ok {
		return false
	}

	return strings.Contains(ws, permission)
}

func (id *Identity) GetIP() string {
	return id.IPs[0]
}

func (id *Identity) HasGroup(group string) bool {
	for _, g := range id.Groups {
		if g == group {
			return true
		}
	}

	return false
}
