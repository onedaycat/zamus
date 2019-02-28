package random

import (
	"encoding/json"
	"strings"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/query"
)

type queryBuilder struct {
	query *query.Query
}

func Query() *queryBuilder {
	username := random.Noun()
	email := random.Email()
	ip := random.IpV4Address()
	return &queryBuilder{
		query: &query.Query{
			Function: random.SillyName(),
			Identity: &invoke.Identity{
				Sub:      username,
				SourceIP: []string{ip},
				Groups:   []string{"user"},
				Username: username,
				Claims: invoke.Claims{
					Email:       email,
					Permissions: make(invoke.Permissions),
				},
			},
		},
	}
}

func (b *queryBuilder) Function(fn string) *queryBuilder {
	b.query.Function = fn

	return b
}

func (b *queryBuilder) Arg(v interface{}) *queryBuilder {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	b.query.Args = data

	return b
}

func (b *queryBuilder) Source(v interface{}) *queryBuilder {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	b.query.Sources = data

	return b
}

func (b *queryBuilder) ValidPermission(key string, permissions ...string) *queryBuilder {
	b.query.PermissionKey = key
	b.query.Identity.Claims.Permissions[key] = strings.Join(permissions, ",")

	return b
}

func (b *queryBuilder) InvalidPermission() *queryBuilder {
	pem := random.SillyName()
	b.query.PermissionKey = pem
	b.query.Identity.Claims.Permissions[pem] = strings.Join([]string{random.SillyName(), random.SillyName()}, ",")

	return b
}

func (b *queryBuilder) NoIdentity() *queryBuilder {
	b.query.PermissionKey = ""
	b.query.Identity = nil

	return b
}

func (b *queryBuilder) Build() *query.Query {
	return b.query
}
