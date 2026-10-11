package cache_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"

	cache "github.com/jkratz55/redis-cache/v2"
)

// reservedIDSerializer is a custom implementation illegally claiming a reserved ID.
type reservedIDSerializer struct{}

func (reservedIDSerializer) Marshal(v any) ([]byte, error) { return msgpack.Marshal(v) }

func (reservedIDSerializer) Unmarshal(b []byte, v any) error { return msgpack.Unmarshal(b, v) }

func (reservedIDSerializer) FormatID() uint8 { return 50 }

type customIDSerializer struct{ reservedIDSerializer }

func (customIDSerializer) FormatID() uint8 { return 150 }

func TestFormats_CustomReservedID(t *testing.T) {
	assert.PanicsWithError(t,
		"Serializer cache_test.reservedIDSerializer uses reserved format id 50: custom ids must be in the range 128-255, implement FormatID() accordingly or wrap it with cache.SerializerWithID",
		func() {
			cache.NewFormatsForTest(reservedIDSerializer{}, cache.CodecWithID(200, nopCodec{}), nil, nil)
		})

	assert.Panics(t, func() {
		cache.NewFormatsForTest(&cache.MessagePackSerializer{}, nopCodecReserved{}, nil, nil)
	})

	assert.NotPanics(t, func() {
		cache.NewFormatsForTest(customIDSerializer{}, cache.CodecWithID(200, nopCodec{}), nil, nil)
	})
}

type nopCodec struct{}

func (nopCodec) Compress(data []byte) ([]byte, error) { return data, nil }

func (nopCodec) Decompress(data []byte) ([]byte, error) { return data, nil }

type nopCodecReserved struct{ nopCodec }

func (nopCodecReserved) FormatID() uint8 { return 0 }
