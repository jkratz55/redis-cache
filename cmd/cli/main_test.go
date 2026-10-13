package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
)

// runCLI runs the command with args and returns its exit status and output.
func runCLI(args ...string) (status int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	status = run(context.Background(), args, &out, &errOut)
	return status, out.String(), errOut.String()
}

func TestRun(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()
	ctx := context.Background()
	assert.NoError(t, cache.New(rdb).Set(ctx, "user:42", user, time.Minute))
	assert.NoError(t, rdb.Set(ctx, "legacy", `{"name":"v1"}`, 0).Err())
	assert.NoError(t, rdb.Set(ctx, "corrupt", []byte{0xC1, 0x01, 1, 0, 0xC1}, 0).Err())
	assert.NoError(t, rdb.HSet(ctx, "hash", "field", "value").Err())
	url := "redis://" + s.Addr()

	t.Run("text", func(t *testing.T) {
		status, stdout, stderr := runCLI("-url", url, "user:42", "legacy")
		assert.Equal(t, 0, status)
		assert.Empty(t, stderr)
		assert.Contains(t, stdout, "Key          user:42\nEncoding     MessagePack (ID 1)\nCompression  none (ID 0)\n")
		assert.Contains(t, stdout, "TTL          1m0s\n")
		assert.Contains(t, stdout, "\"Name\": \"Ada\"")
		assert.Contains(t, stdout, "Key          legacy\nEncoding     JSON (detected)\n")
	})

	t.Run("json", func(t *testing.T) {
		status, stdout, _ := runCLI("-url", url, "-o", "json", "user:42", "missing")
		assert.Equal(t, 1, status)
		lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
		if assert.Len(t, lines, 2) {
			assert.Contains(t, lines[0], `"value":`+userJSON)
			assert.Equal(t, `{"key":"missing","found":false}`, lines[1])
		}
	})

	t.Run("exit status 1 when a key can't be shown", func(t *testing.T) {
		for _, key := range []string{"missing", "corrupt", "hash"} {
			status, stdout, _ := runCLI("-url", url, "user:42", key)
			assert.Equal(t, 1, status, key)
			assert.Contains(t, stdout, "Key          "+key+"\n")
		}
	})

	t.Run("REDIS_URL", func(t *testing.T) {
		t.Setenv("REDIS_URL", url)
		status, _, _ := runCLI("user:42")
		assert.Equal(t, 0, status)
	})

	t.Run("-hex", func(t *testing.T) {
		_, stdout, _ := runCLI("-url", url, "-hex", "legacy")
		assert.Contains(t, stdout, "Stored in Redis\n00000000  7b 22 6e 61 6d 65 22 3a")
	})

	t.Run("-max-size", func(t *testing.T) {
		status, stdout, _ := runCLI("-url", url, "-max-size", "1KiB", "user:42")
		assert.Equal(t, 0, status)
		assert.Contains(t, stdout, "Key          user:42\n")
	})
}

func TestRun_Errors(t *testing.T) {
	s := miniredis.RunT(t)
	url := "redis://" + s.Addr()

	tests := []struct {
		name   string
		args   []string
		status int
		stderr string
	}{
		{"help", []string{"-h"}, 0, "Usage: cli [flags] key..."},
		{"no keys", []string{"-url", url}, 2, "Usage: cli [flags] key..."},
		{"unknown flag", []string{"-nope", "key"}, 2, "flag provided but not defined: -nope"},
		{"invalid output", []string{"-url", url, "-o", "yaml", "key"}, 2, `invalid -o "yaml": must be text or json`},
		{"invalid legacy encoding", []string{"-url", url, "-legacy-encoding", "xml", "key"}, 2, `invalid -legacy-encoding "xml"`},
		{"invalid size", []string{"-max-size", "lots", "key"}, 2, "must be a number of bytes"},
		{"invalid url", []string{"-url", "http://localhost", "key"}, 2, "invalid -url"},
		{"unreachable", []string{"-url", "redis://127.0.0.1:1", "-timeout", "200ms", "key"}, 1, "connect to Redis:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, stdout, stderr := runCLI(tt.args...)
			assert.Equal(t, tt.status, status)
			assert.Empty(t, stdout)
			assert.Contains(t, stderr, tt.stderr)
		})
	}
}

func TestRun_HelpDoesntPrintREDIS_URL(t *testing.T) {
	t.Setenv("REDIS_URL", "redis://user:s3cret@localhost:6379/0")
	_, _, stderr := runCLI("-h")
	assert.Contains(t, stderr, "$REDIS_URL")
	assert.NotContains(t, stderr, "s3cret")
}

func TestByteSize(t *testing.T) {
	for v, want := range map[string]byteSize{
		"1048576": 1 << 20,
		"512KiB":  512 << 10,
		"64MiB":   64 << 20,
		"1GiB":    1 << 30,
		"10B":     10,
		"2 MiB":   2 << 20,
	} {
		var s byteSize
		assert.NoError(t, s.Set(v), v)
		assert.Equal(t, want, s, v)
	}
	for _, v := range []string{"", "lots", "-1", "1.5MiB", "64MB", "99999999999GiB"} {
		var s byteSize
		assert.Error(t, s.Set(v), v)
	}

	for s, want := range map[byteSize]string{0: "0", 10: "10B", 512 << 10: "512KiB", 64 << 20: "64MiB", 1 << 30: "1GiB", 1536: "1536B"} {
		assert.Equal(t, want, s.String())
	}
}
