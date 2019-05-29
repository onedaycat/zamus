package zamus

type Retries struct {
    times int
    count int
}

func NewRetries(times int) *Retries {
    return &Retries{
        times: times,
    }
}

func (r *Retries) Retry() bool {
    if r.count >= r.times {
        return false
    }

    r.count++
    return true
}

func (r *Retries) Reset() {
    r.count = 0
}

func (r *Retries) SetTimes(times int) {
    r.count = 0
    r.times = times
}
