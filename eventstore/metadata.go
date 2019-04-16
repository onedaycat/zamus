package eventstore

const (
    MetadaUserID        = "u"
    MetadataIP          = "i"
    MetadaCorrelationID = "c"
    MetadataTxID        = "t"
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
    m[MetadaUserID] = id
    return m
}

func (m Metadata) SetIP(ip string) Metadata {
    m[MetadataIP] = ip
    return m
}

func (m Metadata) SetCorrelationID(id string) Metadata {
    m[MetadaCorrelationID] = id
    return m
}

func (m Metadata) SetTxID(id string) Metadata {
    m[MetadataTxID] = id
    return m
}

func (m Metadata) GetUserID() string {
    return m[MetadaUserID]
}

func (m Metadata) GetIP() string {
    return m[MetadataIP]
}

func (m Metadata) GetCorrelationID() string {
    return m[MetadaCorrelationID]
}

func (m Metadata) GetTxID() string {
    return m[MetadataTxID]
}
