package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessagePackSerializer_AppendMarshal(t *testing.T) {
	s := MessagePackSerializer{}
	type person struct {
		Name string
		Age  int
	}
	value := person{Name: "Billy", Age: 45}

	want, err := s.Marshal(value)
	assert.NoError(t, err)

	prefix := []byte{1, 2, 3}
	got, err := s.AppendMarshal(prefix, value)
	assert.NoError(t, err)
	assert.Equal(t, append([]byte{1, 2, 3}, want...), got)
	assert.Equal(t, []byte{1, 2, 3}, prefix)

	_, err = s.AppendMarshal(nil, make(chan int))
	assert.Error(t, err)
}

func TestAppendMarshal_Fallback(t *testing.T) {
	// JsonSerializer doesn't implement MarshalAppender.
	out, err := appendMarshal(JsonSerializer{}, []byte("prefix:"), "value")
	assert.NoError(t, err)
	assert.Equal(t, `prefix:"value"`, string(out))

	_, err = appendMarshal(JsonSerializer{}, nil, make(chan int))
	assert.Error(t, err)
}

// TestJsonSerializer_V2Defaults pins the encoding/json/v2 behavior documented on
// JsonSerializer, which differs from encoding/json used by v1 of the library.
func TestJsonSerializer_V2Defaults(t *testing.T) {
	s := JsonSerializer{}

	type withDuration struct{ Timeout time.Duration }
	_, err := s.Marshal(withDuration{Timeout: time.Second})
	assert.Error(t, err)
	assert.Error(t, s.Unmarshal([]byte(`{"Timeout":2000000000}`), &withDuration{}))

	type person struct{ Name string }
	var p person
	assert.NoError(t, s.Unmarshal([]byte(`{"name":"Billy"}`), &p))
	assert.Empty(t, p.Name, "names are matched case-sensitively")

	type withSlice struct{ Tags []string }
	data, err := s.Marshal(withSlice{})
	assert.NoError(t, err)
	assert.JSONEq(t, `{"Tags":[]}`, string(data))
}
