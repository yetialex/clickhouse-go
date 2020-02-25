package column

import (
	"fmt"
	"time"

	"github.com/yetialex/clickhouse-go/lib/binary"
)

type DateTime64 struct {
	base
	precision int
	Timezone  *time.Location
}

func (dt *DateTime64) Read(decoder *binary.Decoder) (interface{}, error) {
	nsec, err := decoder.Int64()
	if err != nil {
		return nil, err
	}
	return time.Unix(0, nsec).In(dt.Timezone), nil
}

func (dt *DateTime64) Write(encoder *binary.Encoder, v interface{}) error {
	var timestamp int64
	switch value := v.(type) {
	case time.Time:
		if !value.IsZero() {
			timestamp = value.UnixNano()
		}
	case int16:
		timestamp = int64(value)
	case int32:
		timestamp = int64(value)
	case int64:
		timestamp = value
	case string:
		var err error
		timestamp, err = dt.parse(value)
		if err != nil {
			return err
		}

	case *time.Time:
		if value != nil && !(*value).IsZero() {
			timestamp = (*value).UnixNano()
		}
	case *int16:
		timestamp = int64(*value)
	case *int32:
		timestamp = int64(*value)
	case *int64:
		timestamp = *value
	case *string:
		var err error
		timestamp, err = dt.parse(*value)
		if err != nil {
			return err
		}

	default:
		return &ErrUnexpectedType{
			T:      v,
			Column: dt,
		}
	}

	return encoder.Int64(timestamp)
}

func (dt *DateTime64) parse(value string) (int64, error) {
	tv, err := time.Parse("2006-01-02 15:04:05.999999999", value)
	if err != nil {
		return 0, err
	}
	return time.Date(
		tv.Year(),
		tv.Month(),
		tv.Day(),
		tv.Hour(),
		tv.Minute(),
		tv.Second(),
		tv.Nanosecond(), time.UTC,
	).UnixNano(), nil
}

func parseDateTime64(name, chType string, timezone *time.Location) (*DateTime64, error) {
	var precision int
	if _, err := fmt.Sscanf(chType, "DateTime64(%d)", &precision); err != nil {
		return nil, err
	}
	return &DateTime64{
		base: base{
			name:    name,
			chType:  chType,
			valueOf: columnBaseTypes[time.Time{}],
		},
		precision: precision,
		Timezone:  timezone,
	}, nil
}
