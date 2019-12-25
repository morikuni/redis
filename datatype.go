package redis

// Data is a marker interface for Redis data types.
// Following types implement this interface:
//
// - SimpleString
// - Error
// - Integer
// - BulkString
// - Array
type Data interface {
	redisDataType()
}

// SimpleString represents a single line string.
// It must not contain CR(\r) or LF(\n).
// Basically, it's returned from a server.
type SimpleString string

func (SimpleString) redisDataType() {}

// Error represents an error message.
// Basically, it's returned from a server.
type Error string

// Error implements error interface.
func (e Error) Error() string {
	return string(e)
}

func (Error) redisDataType() {}

// Integer represents an integer.
type Integer int64

func (Integer) redisDataType() {}

// BulkString represents a binary safe string.
// The maximum size of string is 512MB.
type BulkString []byte

func (bs BulkString) String() string {
	return string(bs)
}

func (BulkString) redisDataType() {}

// Array represents a collection of DataTypes.
type Array []Data

func (Array) redisDataType() {}
