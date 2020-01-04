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

	client := NewClient(pool)

	ctx := context.Background()

	sres, err := client.Set(ctx, &SetRequest{
		Key:   "aaa",
		Value: "123",
	})
	require.WantError(t, false, err)
	assert.Equal(t, "OK", sres.value)

	ires, err := client.Incr(ctx, &IncrRequest{
		Key: "aaa",
	})
	require.WantError(t, false, err)
	assert.Equal(t, int64(124), ires.value)

	sres, err = client.Get(ctx, &GetRequest{
		Key: "aaa",
	})
	require.WantError(t, false, err)
	assert.Equal(t, "124", sres.value)
}

func run(b *testing.B, pool *Pool, send, receive bool) {
	ctx := context.Background()

	conn, err := pool.Get(ctx)
	if err != nil {
		b.Fatal(err)
	}

	if send {
		err := conn.Send(ctx, Array{
			BulkString("SET"),
			BulkString("aaa"),
			BulkString("123"),
		})
		if err != nil {
			b.Fatal(err)
		}
	}

	if receive {
		res, err := conn.Receive(ctx)
		if err != nil {
			b.Fatal(err)
		}

		if res != SimpleString("OK") {
			b.Fatal(res)
		}
	}

	err = pool.Put(ctx, conn)
	if err != nil {
		b.Fatal(err)
	}
}

func withIdle(tb testing.TB) *Pool {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		tb.Skip("REDIS_ADDR is empty")
	}

	pool, err := NewPool(addr, MaxIdle(1))
	require.WantError(tb, false, err)

	return pool
}

func noIdle(tb testing.TB) *Pool {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		tb.Skip("REDIS_ADDR is empty")
	}

	pool, err := NewPool(addr, MaxIdle(0))
	require.WantError(tb, false, err)

	return pool
}

func BenchmarkE2E(b *testing.B) {
	cases := map[string]struct {
		connFunc func(testing.TB) *Pool
		send     bool
		receive  bool
	}{
		"idle":                 {connFunc: withIdle, send: false, receive: false},
		"no idle":              {connFunc: noIdle, send: false, receive: false},
		"send idle":            {connFunc: withIdle, send: true, receive: false},
		"send no idle":         {connFunc: noIdle, send: true, receive: false},
		"send receive idle":    {connFunc: withIdle, send: true, receive: true},
		"send receive no idle": {connFunc: noIdle, send: true, receive: true},
	}

	for name, bc := range cases {
		bc := bc

		b.Run(name, func(b *testing.B) {
			conn := bc.connFunc(b)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run(b, conn, bc.send, bc.receive)
			}
		})
	}
}
