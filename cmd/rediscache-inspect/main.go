// Command rediscache-inspect fetches values written by redis-cache from Redis and
// prints how each value is encoded and compressed, along with the decoded data.
//
// Usage:
//
//	go install github.com/jkratz55/redis-cache/v2/cmd/rediscache-inspect@latest
//	rediscache-inspect [flags] key...
//
// Values are decoded without their Go types: MessagePack and JSON values are
// printed as JSON, keeping the order of their fields, and Protobuf values are
// printed without a schema, like protoc --decode_raw. Values written before v2,
// which have no format header, are detected, or decoded as set by the
// -legacy-encoding and -legacy-compression flags. Anything else is printed as text
// or as a hex dump.
//
// To inspect every key matching a pattern, combine it with redis-cli:
//
//	redis-cli --scan --pattern 'user:*' | xargs rediscache-inspect
//
// The exit status is 0 if every key was found and decoded, 1 if a key was
// missing, held another Redis type, couldn't be decoded, or Redis failed, and 2
// for invalid flags.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// config holds the command-line flags.
type config struct {
	url               string
	cluster           bool
	output            string
	timeout           time.Duration
	maxSize           byteSize
	legacyEncoding    string
	legacyCompression string
	hex               bool
}

// run runs the command with args and returns its exit status.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg := config{maxSize: 64 << 20}
	fs := flag.NewFlagSet("rediscache-inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: rediscache-inspect [flags] key...\n\n"+
			"Fetches values written by redis-cache and prints their encoding, compression, and data.\n\n"+
			"Flags:\n")
		fs.PrintDefaults()
	}

	// The default isn't taken from $REDIS_URL here, since the usage message would
	// print the password it may contain.
	fs.StringVar(&cfg.url, "url", "",
		"Redis `URL`, such as redis://user:password@host:6379/0 or rediss:// for TLS (default $REDIS_URL or redis://localhost:6379/0)")
	fs.BoolVar(&cfg.cluster, "cluster", false,
		"connect to a Redis Cluster, add more nodes to -url with addr parameters: redis://host1:6379?addr=host2:6379")
	fs.StringVar(&cfg.output, "o", "text", "output `format`: text, or json for one JSON object per key")
	fs.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "timeout of each Redis command")
	fs.Var(&cfg.maxSize, "max-size", "maximum decompressed `size` of a value, such as 64MiB or 512KiB")
	fs.StringVar(&cfg.legacyEncoding, "legacy-encoding", "auto",
		"encoding of values without a format header: auto, "+strings.Join(legacyEncodings, ", "))
	fs.StringVar(&cfg.legacyCompression, "legacy-compression", "auto",
		"compression of values without a format header: auto, "+strings.Join(codecFlagNames(), ", "))
	fs.BoolVar(&cfg.hex, "hex", false, "also print a hex dump of the value stored in Redis")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	keys := fs.Args()
	if len(keys) == 0 {
		fs.Usage()
		return 2
	}
	if cfg.url == "" {
		cfg.url = cmp.Or(os.Getenv("REDIS_URL"), "redis://localhost:6379/0")
	}

	var render func(inspection) error
	switch cfg.output {
	case "text":
		render = newTextRenderer(stdout).render
	case "json":
		render = newJSONRenderer(stdout).render
	default:
		fmt.Fprintf(stderr, "invalid -o %q: must be text or json\n", cfg.output)
		return 2
	}

	dec, err := newDecoder(cfg.maxSize, cfg.legacyEncoding, cfg.legacyCompression)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	defer dec.Close()

	rdb, err := connect(cfg)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	defer rdb.Close()

	pingCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	err = rdb.Ping(pingCtx).Err()
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "connect to Redis: %v\n", err)
		return 1
	}

	status := 0
	for _, key := range keys {
		ins := fetch(ctx, rdb, dec, key, cfg)
		if err := render(ins); err != nil {
			fmt.Fprintf(stderr, "write output: %v\n", err)
			return 1
		}
		if !ins.ok() {
			status = 1
		}
	}
	return status
}

// connect creates the Redis client described by cfg.
func connect(cfg config) (redis.UniversalClient, error) {
	if cfg.cluster {
		opts, err := redis.ParseClusterURL(cfg.url)
		if err != nil {
			return nil, fmt.Errorf("invalid -url: %w", err)
		}
		return redis.NewClusterClient(opts), nil
	}
	opts, err := redis.ParseURL(cfg.url)
	if err != nil {
		return nil, fmt.Errorf("invalid -url: %w", err)
	}
	return redis.NewClient(opts), nil
}

// fetch reads key and its TTL from Redis and decodes its value.
func fetch(ctx context.Context, rdb redis.UniversalClient, dec *decoder, key string, cfg config) inspection {
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	ins := inspection{key: key, showHex: cfg.hex}
	data, err := rdb.Get(ctx, key).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		return ins
	case err != nil && strings.HasPrefix(err.Error(), "WRONGTYPE"):
		ins.found = true
		ins.redisType, err = rdb.Type(ctx, key).Result()
		if err != nil {
			ins.err = fmt.Errorf("redis: %w", err)
		}
		return ins
	case err != nil:
		ins.err = fmt.Errorf("redis: %w", err)
		return ins
	}

	ins.found = true
	ins.ttl, err = rdb.PTTL(ctx, key).Result()
	if err != nil {
		ins.err = fmt.Errorf("redis: %w", err)
		return ins
	}
	ins.result = dec.inspect(data)
	return ins
}

// byteSize is a flag holding a number of bytes, with an optional unit such as KiB,
// MiB, or GiB.
type byteSize uint64

var byteUnits = []struct {
	suffix string
	size   uint64
}{
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
	{"B", 1},
}

func (s *byteSize) String() string {
	for _, u := range byteUnits {
		if uint64(*s) >= u.size && uint64(*s)%u.size == 0 {
			return strconv.FormatUint(uint64(*s)/u.size, 10) + u.suffix
		}
	}
	return "0"
}

func (s *byteSize) Set(v string) error {
	unit := uint64(1)
	for _, u := range byteUnits {
		if num, ok := strings.CutSuffix(v, u.suffix); ok {
			v, unit = strings.TrimSpace(num), u.size
			break
		}
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n > math.MaxUint64/unit {
		return errors.New("must be a number of bytes, such as 1048576, 512KiB, 64MiB, or 1GiB")
	}
	*s = byteSize(n * unit)
	return nil
}
