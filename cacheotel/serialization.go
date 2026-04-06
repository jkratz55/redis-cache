package cacheotel

import (
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

type InstrumentedSerializer struct {
	serializer cache.Serializer
	duration   metric.Float64Histogram
	errs       metric.Int64Counter
}

func NewInstrumentedSerializer(serializer cache.Serializer) *InstrumentedSerializer {

}

func (i *InstrumentedSerializer) Marshal(i2 interface{}) ([]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (i *InstrumentedSerializer) Unmarshal(bytes []byte, i2 interface{}) error {
	// TODO implement me
	panic("implement me")
}
