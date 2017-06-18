package db

import (
	"time"
)

type SqlLiteTimestamp []byte

func (vSelf SqlLiteTimestamp) Time() (time.Time, error) {
    return time.Parse("2006-01-02 15:04:05.999999999-07:00", string(vSelf))
}

func (vSelf SqlLiteTimestamp) IsNull() (bool) {
	return len(vSelf) == 0
}
