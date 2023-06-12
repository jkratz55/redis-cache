package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type (
	ProcessHook    func(ctx context.Context, cmd redis.Cmder) error
	CompressHook   func(c Codec) ([]byte, error)
	DecompressHook func(c Codec) ([]byte, error)
	MarshalHook    func(m Marshaller) ([]byte, error)
	UnmarshalHook  func(um Unmarshaller) error
)

type Hook interface {
	ProcessHook(next ProcessHook) ProcessHook
	CompressHook(next CompressHook) CompressHook
	DecompressHook(next DecompressHook) DecompressHook
	MarshalHook(next MarshalHook) MarshalHook
	UnmarshalHook(next UnmarshalHook) UnmarshalHook
}

type hooksMixin struct {
	hooks []Hook
}
