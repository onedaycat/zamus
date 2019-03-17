package eventstore

const (
	METADATA_USER_ID        = "u"
	METADATA_IP             = "i"
	METADATA_CORRELATIOM_ID = "c"
	METADATA_TX_ID          = "t"
)

type Metadata map[string]string

func NewMetadata() Metadata {
	return make(Metadata, 10)
}

func (m Metadata) Get(key string) string {
	return m[key]
}

func (m Metadata) Set(key, val string) Metadata {
	m[key] = val
	return m
}

func (m Metadata) SetUserID(id string) Metadata {
	m[METADATA_USER_ID] = id
	return m
}

func (m Metadata) SetIP(ip string) Metadata {
	m[METADATA_IP] = ip
	return m
}

func (m Metadata) SetCorrelationID(id string) Metadata {
	m[METADATA_CORRELATIOM_ID] = id
	return m
}

func (m Metadata) SetTxID(id string) Metadata {
	m[METADATA_TX_ID] = id
	return m
}

func (m Metadata) GetUserID() string {
	return m[METADATA_USER_ID]
}

func (m Metadata) GetIP() string {
	return m[METADATA_IP]
}

func (m Metadata) GetCorrelationID() string {
	return m[METADATA_CORRELATIOM_ID]
}

func (m Metadata) GetTxID() string {
	return m[METADATA_TX_ID]
}
