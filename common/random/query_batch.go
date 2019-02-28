package random

import (
	"encoding/json"
	"strings"

	random "github.com/Pallinder/go-randomdata"
	"github.com/onedaycat/zamus/invoke"
	"github.com/onedaycat/zamus/query"
)

type batchQueryBuilder struct {
	query *query.Query
}

func BatchQuery() *batchQueryBuilder {
	username := random.Noun()
	email := random.Email()
	ip := random.IpV4Address()
	bb := &batchQueryBuilder{
		query: &query.Query{
			Function:      random.SillyName(),
			NBatchSources: 1,
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

	bb.RandomSources(1)

	return bb
}

func (b *batchQueryBuilder) Function(fn string) *batchQueryBuilder {
	b.query.Function = fn

	return b
}

func (b *batchQueryBuilder) Arg(v interface{}) *batchQueryBuilder {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	b.query.Args = data

	return b
}

func (b *batchQueryBuilder) RandomSources(n int) *batchQueryBuilder {
	sources := newRandomSources(n)
	data, err := json.Marshal(sources)
	if err != nil {
		panic(err)
	}

	b.query.Sources = data
	b.query.NBatchSources = n

	return b
}

func (b *batchQueryBuilder) Sources(sources ...interface{}) *batchQueryBuilder {
	data, err := json.Marshal(sources)
	if err != nil {
		panic(err)
	}

	b.query.Sources = data
	b.query.NBatchSources = len(sources)

	return b
}

func (b *batchQueryBuilder) ValidPermission(key string, permissions ...string) *batchQueryBuilder {
	b.query.PermissionKey = key
	b.query.Identity.Claims.Permissions[key] = strings.Join(permissions, ",")

	return b
}

func (b *batchQueryBuilder) InvalidPermission() *batchQueryBuilder {
	pem := random.SillyName()
	b.query.PermissionKey = pem
	b.query.Identity.Claims.Permissions[pem] = strings.Join([]string{random.SillyName(), random.SillyName()}, ",")

	return b
}

func (b *batchQueryBuilder) Build() *query.Query {
	return b.query
}

func (b *batchQueryBuilder) BuildJSON() []byte {
	data, err := json.Marshal(b.query)
	if err != nil {
		panic(err)
	}

	return data
}

type randomSource struct {
	ID   string
	Name string
}

func newRandomSources(n int) []*randomSource {
	rs := make([]*randomSource, n)
	for i := 0; i < n; i++ {
		rs[i] = &randomSource{
			ID:   random.SillyName(),
			Name: random.SillyName(),
		}
	}

	return rs
}
