package redis

import (
	"errors"
	"strconv"
)

type StringResponse struct {
	value string
}

func (res *StringResponse) String() string {
	return res.value
}

func (res *StringResponse) FromData(data Data) error {
	switch t := data.(type) {
	case SimpleString:
		res.value = string(t)
	case Error:
		return t
	case Integer:
		res.value = strconv.FormatInt(int64(t), 10)
	case BulkString:
		res.value = string(t)
	case Array:
		return errors.New("expected string but array")
	}

	return nil
}

type IntegerResponse struct {
	value int64
}

func (res *IntegerResponse) String() string {
	return strconv.FormatInt(res.value, 10)
}

func (res *IntegerResponse) FromData(data Data) error {
	switch t := data.(type) {
	case SimpleString:
		i, err := strconv.ParseInt(string(t), 10, 64)
		if err != nil {
			return err
		}
		res.value = i
	case Error:
		return t
	case Integer:
		res.value = int64(t)
	case BulkString:
		i, err := strconv.ParseInt(string(t), 10, 64)
		if err != nil {
			return err
		}
		res.value = i
	case Array:
		return errors.New("expected integer but array")
	}

	return nil
}

const Discard = discardResponse(0)

type discardResponse int

func (discardResponse) FromData(data Data) error {
	switch t := data.(type) {
	case Error:
		return t
	default:
		return nil
	}
}
