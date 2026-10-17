// Package benchmarks compares the CPU time, allocations and encoded size of every
// Serializer and CompressionCodec provided by the library, and of Cache operations
// using them. The results and guidance on choosing between them are published in
// the README.
//
// Run the benchmarks with:
//
//	go test -run '^$' -bench . -benchmem ./internal/benchmarks
//
// Every serializer encodes the same orders, generated with a fixed seed, and the
// codecs compress them as encoded by MessagePackSerializer, the default.
package benchmarks

//go:generate protoc --go_out=benchpb --go_opt=paths=source_relative --proto_path=benchpb order.proto

import (
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/jkratz55/redis-cache/v2/internal/benchmarks/benchpb"
)

// Order is a representative cached value: a record with nested structs, slices, a
// map, numbers and free text.
type Order struct {
	ID              int64
	CustomerID      string
	Status          string
	CreatedAt       int64
	ShippingAddress Address
	Items           []LineItem
	Tags            []string
	Attributes      map[string]string
	Total           float64
	Notes           string
}

// LineItem is an item of an Order.
type LineItem struct {
	SKU       string
	Name      string
	Quantity  int32
	UnitPrice float64
}

// Address is the shipping address of an Order.
type Address struct {
	Line1      string
	City       string
	State      string
	PostalCode string
	Country    string
}

// Payload is a named Order of a given size.
type Payload struct {
	// Name identifies the payload in benchmark names: small, medium or large.
	Name  string
	Order Order
}

// Payloads returns the orders encoded by the benchmarks. Encoded with
// MessagePackSerializer, the small order is about 350 bytes, the medium order
// about 2 KiB, and the large order about 64 KiB.
func Payloads() []Payload {
	return []Payload{
		{Name: "small", Order: NewOrder(1, 1, 0)},
		{Name: "medium", Order: NewOrder(2, 21, 2)},
		{Name: "large", Order: NewOrder(3, 800, 20)},
	}
}

var (
	adjectives = []string{"Classic", "Premium", "Compact", "Wireless", "Organic", "Vintage", "Ultra", "Smart"}
	products   = []string{"Headphones", "Backpack", "Coffee Beans", "Desk Lamp", "Water Bottle", "Keyboard", "Notebook", "Sneakers"}
	colors     = []string{"Black", "White", "Navy", "Olive", "Red", "Graphite", "Sand", "Teal"}
	statuses   = []string{"pending", "paid", "shipped", "delivered"}
	tags       = []string{"gift", "priority", "subscription", "international", "fragile", "promo", "repeat-customer"}
	cities     = []string{"Austin", "Denver", "Portland", "Chicago", "Boston", "Seattle"}
	states     = []string{"TX", "CO", "OR", "IL", "MA", "WA"}
	sentences  = []string{
		"Please leave the package at the side door.",
		"Customer requested gift wrapping for every item.",
		"Deliver after 5pm on weekdays.",
		"Contact the customer before shipping any backordered items.",
		"Items may ship separately from different warehouses.",
	}
)

// NewOrder returns an order generated from seed with the given number of items and
// sentences of notes. The same arguments always return the same order.
func NewOrder(seed uint64, items, notes int) Order {
	rng := rand.New(rand.NewPCG(seed, seed))
	pick := func(values []string) string { return values[rng.IntN(len(values))] }

	order := Order{
		ID:         rng.Int64N(1 << 40),
		CustomerID: fmt.Sprintf("cus_%012d", rng.Int64N(1e12)),
		Status:     pick(statuses),
		CreatedAt:  1_760_000_000_000_000_000 + rng.Int64N(1e15),
		ShippingAddress: Address{
			Line1:      fmt.Sprintf("%d %s Street", rng.IntN(9000)+100, pick(adjectives)),
			City:       pick(cities),
			State:      pick(states),
			PostalCode: fmt.Sprintf("%05d", rng.IntN(100000)),
			Country:    "US",
		},
		Attributes: make(map[string]string),
	}
	for range items {
		item := LineItem{
			SKU:       fmt.Sprintf("SKU-%06d", rng.IntN(1_000_000)),
			Name:      fmt.Sprintf("%s %s - %s", pick(adjectives), pick(products), pick(colors)),
			Quantity:  int32(rng.IntN(5) + 1),
			UnitPrice: float64(rng.IntN(20000)+99) / 100,
		}
		order.Items = append(order.Items, item)
		order.Total += float64(item.Quantity) * item.UnitPrice
	}
	for range min(items, 3) + 1 {
		order.Tags = append(order.Tags, pick(tags))
	}
	order.Attributes["channel"] = pick([]string{"web", "ios", "android"})
	order.Attributes["warehouse"] = fmt.Sprintf("wh-%02d", rng.IntN(20))
	if items > 1 {
		order.Attributes["coupon"] = fmt.Sprintf("SAVE%d", rng.IntN(50))
	}
	var b strings.Builder
	for i := range notes {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(pick(sentences))
	}
	order.Notes = b.String()
	return order
}

// Proto returns o as the equivalent Protobuf message.
func (o Order) Proto() *benchpb.Order {
	msg := &benchpb.Order{
		Id:         o.ID,
		CustomerId: o.CustomerID,
		Status:     o.Status,
		CreatedAt:  o.CreatedAt,
		ShippingAddress: &benchpb.Address{
			Line1:      o.ShippingAddress.Line1,
			City:       o.ShippingAddress.City,
			State:      o.ShippingAddress.State,
			PostalCode: o.ShippingAddress.PostalCode,
			Country:    o.ShippingAddress.Country,
		},
		Tags:       o.Tags,
		Attributes: o.Attributes,
		Total:      o.Total,
		Notes:      o.Notes,
	}
	for _, item := range o.Items {
		msg.Items = append(msg.Items, &benchpb.LineItem{
			Sku:       item.SKU,
			Name:      item.Name,
			Quantity:  item.Quantity,
			UnitPrice: item.UnitPrice,
		})
	}
	return msg
}

// Incompressible returns n bytes that can't be compressed, generated from seed,
// such as an image or an encrypted value.
func Incompressible(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, seed))
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	return data
}
