package redis

import (
	"context"
	"time"
)

type Doer interface {
	Do(ctx context.Context, req Request, res Response) error
}

type doFunc func(ctx context.Context, req Request, res Response) error

func (f doFunc) Do(ctx context.Context, req Request, res Response) error {
	return f(ctx, req, res)
}

type GetRequest struct {
	Key string
}

func (req *GetRequest) ToData() (Data, error) {
	return Array{
		BulkString("GET"),
		BulkString(req.Key),
	}, nil
}

func Get(ctx context.Context, do Doer, req *GetRequest) (*StringResponse, error) {
	var res StringResponse
	return &res, do.Do(ctx, req, &res)
}

type SetRequest struct {
	Key          string
	Value        string
	Expire       time.Duration
	NotExist     bool
	AlreadyExist bool
}

func (req *SetRequest) ToData() (Data, error) {
	d := Array{
		BulkString("SET"),
		BulkString(req.Key),
		BulkString(req.Value),
	}

	if req.Expire > 0 {
		d = append(d, BulkString("PX"), Integer(req.Expire)/Integer(time.Millisecond))
	}

	switch {
	case req.NotExist:
		d = append(d, BulkString("NX"))
	case req.AlreadyExist:
		d = append(d, BulkString("XX"))
	}

	return d, nil
}

func Set(ctx context.Context, do Doer, req *SetRequest) (*StringResponse, error) {
	var res StringResponse
	return &res, do.Do(ctx, req, &res)
}

type IncrRequest struct {
	Key string
}

func (req *IncrRequest) ToData() (Data, error) {
	return Array{
		BulkString("INCR"),
		BulkString(req.Key),
	}, nil
}

func Incr(ctx context.Context, do Doer, req *IncrRequest) (*IntegerResponse, error) {
	var res IntegerResponse
	return &res, do.Do(ctx, req, &res)
}
