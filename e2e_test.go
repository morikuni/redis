package redis

import (
	"context"
	"os"
	"testing"

	"github.com/morikuni/redis/internal/assert"
	"github.com/morikuni/redis/internal/require"
)

func TestE2E(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR is empty")
	}

	pool, err := NewPool(addr)
	require.WantError(t, false, err)

	ctx := context.Background()
	conn, err := pool.Get(ctx)
	require.WantError(t, false, err)

	err = conn.Send(ctx, Array{
		BulkString("SET"),
		BulkString("aaa"),
		BulkString("123"),
	})
	require.WantError(t, false, err)

	res, err := conn.Receive(ctx)
	require.WantError(t, false, err)
	assert.Equal(t, SimpleString("OK"), res)

	err = conn.Send(ctx, Array{
		BulkString("INCR"),
		BulkString("aaa"),
	})
	require.WantError(t, false, err)

	res, err = conn.Receive(ctx)
	require.WantError(t, false, err)
	assert.Equal(t, Integer(124), res)

	err = conn.Send(ctx, Array{
		BulkString("GET"),
		BulkString("aaa"),
	})
	require.WantError(t, false, err)

	res, err = conn.Receive(ctx)
	require.WantError(t, false, err)
	assert.Equal(t, BulkString("124"), res)

	err = conn.Send(ctx, Array{
		BulkString("ERROR"),
	})
	require.WantError(t, false, err)

	res, err = conn.Receive(ctx)
	require.WantError(t, false, err)
	assert.Equal(t, Error("ERR unknown command 'ERROR'"), res)

	err = conn.Send(ctx, Array{
		BulkString("MGET"),
		BulkString("aaa"),
		BulkString("bbb"),
		BulkString("aaa"),
	})
	require.WantError(t, false, err)

	res, err = conn.Receive(ctx)
	require.WantError(t, false, err)
	assert.Equal(t, Array{
		BulkString("124"),
		BulkString(nil),
		BulkString("124"),
	}, res)
}
