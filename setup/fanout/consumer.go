package main

type Consumers struct {
	HashKey   string       `json:"h"`
	Consumers []*Consumers `json:"c"`
}

type Consumer struct {
	Name           string   `json:"n"`
	DestinationArn string   `json:"d"`
	Enable         bool     `json:"o"`
	EventTypes     []string `json:"e"`
	CreatedAt      int64    `json:"c"`
	UpdatedAt      int64    `json:"u"`
}
