package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCacheable(t *testing.T) {

	type testDefinition struct {
		name        string
		key         string
		initializer func(c *Cache) error
		fn          func(ctx context.Context) (int, error)
		expected    int
		shouldErr   bool
	}

	tests := []testDefinition{
		{
			name: "Cache Miss",
			key:  "user:43234:grade",
			initializer: func(c *Cache) error {
				return nil
			},
			fn: func(ctx context.Context) (int, error) {
				// simulate really slow computation or IO operation
				time.Sleep(2 * time.Second)
				return 42, nil
			},
			expected:  42,
			shouldErr: false,
		},
		{
			name: "Cache Hit",
			key:  "user:123:grade",
			initializer: func(c *Cache) error {
				err := c.Set(context.Background(), "user:123:grade", 95, 0)
				assert.NoError(t, err)
				return nil
			},
			fn: func(ctx context.Context) (int, error) {
				// simulate really slow computation or IO operation
				time.Sleep(2 * time.Second)
				// Initial to ensure the value comes from the cache
				return 0, nil
			},
			expected:  95,
			shouldErr: false,
		},
		{
			name: "Cache Miss with Error",
			key:  "user:123:grade",
			initializer: func(c *Cache) error {
				return nil
			},
			fn: func(ctx context.Context) (int, error) {
				return 0, assert.AnError
			},
			expected:  0,
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setup()
			defer tearDown()

			cache := New(client)
			err := test.initializer(cache)
			assert.NoError(t, err)

			res, err := Cacheable(context.Background(),
				cache,
				test.key,
				100*time.Millisecond,
				10*time.Minute,
				test.fn,
			)
			if test.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, res)
			}

			// Wait for async cache updates to complete
			time.Sleep(1 * time.Second)

			if !test.shouldErr {
				// Ensure values are stored in the cache
				var target int
				err = cache.Get(context.Background(), test.key, &target)
				assert.NoError(t, err)
				assert.Equal(t, test.expected, target)
			}
		})
	}
}

func TestWrite(t *testing.T) {

	type testDefinition struct {
		name                string
		key                 string
		val                 int
		initializer         func(c *Cache) error
		fn                  func(ctx context.Context, val int) error
		shouldErr           bool
		expectedCachedValue int
	}

	tests := []testDefinition{
		{
			name:                "Write Success - Empty Cache",
			key:                 "user:123:grade",
			expectedCachedValue: 55,
			val:                 55,
			initializer: func(c *Cache) error {
				return nil
			},
			fn: func(ctx context.Context, val int) error {
				return nil
			},
			shouldErr: false,
		},
		{
			name: "Write Success - Overwrite Cache",
			key:  "user:123:grade",
			val:  95,
			initializer: func(c *Cache) error {
				assert.NoError(t, c.Set(context.Background(), "user:123:grade", 90, 0))
				return nil
			},
			fn: func(ctx context.Context, val int) error {
				return nil
			},
			shouldErr:           false,
			expectedCachedValue: 95,
		},
		{
			name:                "Write Failure",
			key:                 "user:123:grade",
			val:                 95,
			expectedCachedValue: 0,
			initializer: func(c *Cache) error {
				return nil
			},
			fn: func(ctx context.Context, val int) error {
				return assert.AnError
			},
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setup()
			defer tearDown()

			cache := New(client)
			err := test.initializer(cache)
			assert.NoError(t, err)

			err = Write(
				context.Background(),
				cache,
				test.key,
				test.val,
				10*time.Minute,
				test.fn,
			)
			if test.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if !test.shouldErr {
				// Ensure values are stored in the cache
				var target int
				err = cache.Get(context.Background(), test.key, &target)
				assert.NoError(t, err)
				assert.Equal(t, test.expectedCachedValue, target)
			}
		})
	}
}

func TestDelete(t *testing.T) {

	fn := func(ctx context.Context, val int) error {
		return nil
	}

	setup()
	defer tearDown()

	cache := New(client)

	assert.NoError(t, cache.Set(context.Background(), "user:123:grade", 95, 0))

	err := Delete(context.Background(), cache, "user:123:grade", 44, fn)
	assert.NoError(t, err)
}
