package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

type record struct {
	ID         string
	FirstName  string
	MiddleName string
	LastName   string
}

func (r *record) Marshall() ([]byte, error) {
	return msgpack.Marshal(*r)
}

func (r *record) Unmarshall(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func TestTypesafeCache_Set(t *testing.T) {
	setup()
	defer tearDown()

	tc := &TypesafeCache[*record]{
		redis: client,
	}

	rec := record{
		ID:         "123",
		FirstName:  "Billy",
		MiddleName: "Joe",
		LastName:   "Bob",
	}

	err := tc.Set(context.Background(), "rec", &rec)
	assert.NoError(t, err)

	res, err := client.Get(context.Background(), "rec").Bytes()
	assert.NoError(t, err)
	assert.NotEmpty(t, res)

	expected, err := rec.Marshall()
	assert.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestTypesafeCache_Get(t *testing.T) {
	setup()
	defer tearDown()

	tc := &TypesafeCache[*record]{
		redis: client,
	}

	rec := record{
		ID:         "123",
		FirstName:  "Billy",
		MiddleName: "Silly",
		LastName:   "Bob",
	}
	data, err := rec.Marshall()
	assert.NoError(t, err)

	err = client.Set(context.Background(), "key123", data, 0).Err()
	assert.NoError(t, err)

	var res record
	err = tc.Get(context.Background(), "key123", &res)
	assert.NoError(t, err)
	assert.NotNil(t, rec)
	assert.Equal(t, rec, res)
}
