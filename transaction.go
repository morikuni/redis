package redis

import (
	"context"
	"errors"
)

type Transaction struct {
	p                *Pipeline
	pendingResponses []Response
}

func (tx *Transaction) Do(ctx context.Context, req Request, res Response) error {
	tx.pendingResponses = append(tx.pendingResponses, res)
	return tx.p.Async().Do(ctx, req, Discard)
}

func (tx *Transaction) Exec(ctx context.Context) error {
	execRes := execResponse{tx.pendingResponses}
	err := tx.p.Async().Do(ctx, exec, &execRes)
	if err != nil {
		return err
	}

	return tx.p.Await(ctx)
}

func (tx *Transaction) Discard(ctx context.Context) error {
	panic("implement me")
}

type multiRequest struct{}

var multi multiRequest

func (req multiRequest) ToData() (Data, error) {
	return Array{BulkString("MULTI")}, nil
}

type execRequest struct{}

var exec execRequest

func (req execRequest) ToData() (Data, error) {
	return Array{BulkString("EXEC")}, nil
}

type execResponse struct {
	responses []Response
}

func (res *execResponse) FromData(data Data) error {
	switch t := data.(type) {
	case Array:
		for i, d := range t {
			err := res.responses[i].FromData(d)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("expected array")
	}

	return nil
}
