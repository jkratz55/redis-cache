package main

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// textRenderer prints inspections for people reading a terminal.
type textRenderer struct {
	w     io.Writer
	color bool
	first bool
}

func newTextRenderer(w io.Writer) *textRenderer {
	return &textRenderer{w: w, color: isTerminal(w) && os.Getenv("NO_COLOR") == "", first: true}
}

// isTerminal reports whether w is a terminal.
func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func (t *textRenderer) render(ins inspection) error {
	var b strings.Builder
	if !t.first {
		b.WriteString("\n")
	}
	t.first = false

	field := func(label, value string) {
		fmt.Fprintf(&b, "%-12s %s\n", label, value)
	}
	field("Key", t.style(ins.key, "1"))

	switch {
	case ins.err != nil:
		field("Error", t.style(ins.err.Error(), "31"))
		return t.write(b.String())
	case !ins.found:
		field("Status", t.style("not found", "31"))
		return t.write(b.String())
	case ins.redisType != "":
		field("Status", t.style(fmt.Sprintf("holds a Redis %s, not a value written by redis-cache", ins.redisType), "31"))
		return t.write(b.String())
	}

	field("Encoding", formatText(ins.encoding))
	field("Compression", formatText(ins.compression))
	size := formatSize(len(ins.stored)) + " in Redis"
	if ins.decodedSize >= 0 && ins.compression.name != "none" {
		size += ", " + formatSize(ins.decodedSize) + " decompressed"
	}
	field("Size", size)
	field("TTL", formatTTL(ins.ttl))
	if ins.header {
		field("Header", "v1")
	} else {
		field("Header", "none, written before v2 or not by redis-cache")
	}
	if ins.result.err != nil {
		field("Error", t.style(ins.result.err.Error(), "31"))
	}
	if ins.hint != "" {
		field("Note", ins.hint)
	}

	b.WriteString("\n")
	switch {
	case ins.value != nil:
		v := jsontext.Value(bytes.Clone(ins.value))
		if err := v.Indent(append(jsonOptions, jsontext.WithIndent("  "))...); err != nil {
			return err
		}
		b.Write(v)
		b.WriteString("\n")
	case ins.text != "":
		b.WriteString(ins.text)
		if !strings.HasSuffix(ins.text, "\n") {
			b.WriteString("\n")
		}
	}
	if ins.showHex {
		fmt.Fprintf(&b, "\n%s\n%s", t.style("Stored in Redis", "1"), hexDump(ins.stored))
	}
	return t.write(b.String())
}

// style wraps s in the ANSI SGR code if colors are enabled.
func (t *textRenderer) style(s, code string) string {
	if !t.color {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

func (t *textRenderer) write(s string) error {
	_, err := io.WriteString(t.w, s)
	return err
}

// formatText formats f for the text output, such as "Zstandard (ID 6)" or
// "JSON (detected)".
func formatText(f format) string {
	var details []string
	if f.id >= 0 {
		details = append(details, fmt.Sprintf("ID %d", f.id))
	}
	if f.note != "" {
		details = append(details, f.note)
	}
	if len(details) == 0 {
		return f.name
	}
	return f.name + " (" + strings.Join(details, ", ") + ")"
}

// formatSize formats n bytes, such as 312 B or 1.2 KiB.
func formatSize(n int) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, exp := float64(n)/unit, 0
	for value >= unit && exp < 2 {
		value /= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", value, "KMG"[exp])
}

// formatTTL formats the time to live of a key, which is negative if the key
// doesn't expire.
func formatTTL(ttl time.Duration) string {
	switch {
	case ttl < 0:
		return "none"
	case ttl < time.Second:
		return ttl.String()
	}
	return ttl.Round(time.Second).String()
}

// jsonRenderer prints every inspection as a JSON object on its own line.
type jsonRenderer struct {
	w io.Writer
}

func newJSONRenderer(w io.Writer) *jsonRenderer {
	return &jsonRenderer{w: w}
}

// jsonInspection is the JSON output of an inspection.
type jsonInspection struct {
	Key   string `json:"key"`
	Found bool   `json:"found"`
	// RedisType is set if the key isn't a string.
	RedisType string `json:"redis_type,omitzero"`
	// TTL is the time to live in milliseconds, or -1 if the key doesn't expire.
	TTL         *int64      `json:"ttl_ms,omitzero"`
	Size        *int        `json:"size,omitzero"`
	Header      *bool       `json:"header,omitzero"`
	Encoding    *jsonFormat `json:"encoding,omitzero"`
	Compression *jsonFormat `json:"compression,omitzero"`
	DecodedSize *int        `json:"decoded_size,omitzero"`
	// Value is the value as JSON, if it has a JSON form. Otherwise Text holds the
	// value as text.
	Value jsontext.Value `json:"value,omitzero"`
	Text  string         `json:"text,omitzero"`
	Hint  string         `json:"note,omitzero"`
	Error string         `json:"error,omitzero"`
}

type jsonFormat struct {
	Name string `json:"name"`
	// ID is the format ID from the header, omitted for values without a header.
	ID   *int   `json:"id,omitzero"`
	Note string `json:"note,omitzero"`
}

func (j *jsonRenderer) render(ins inspection) error {
	out := jsonInspection{Key: ins.key, Found: ins.found, RedisType: ins.redisType}
	if ins.err != nil {
		out.Error = ins.err.Error()
	}
	if ins.found && ins.redisType == "" && ins.err == nil {
		ttl := ins.ttl.Milliseconds()
		if ins.ttl < 0 {
			ttl = -1
		}
		out.TTL = &ttl
		out.Size = new(len(ins.stored))
		out.Header = new(ins.header)
		out.Encoding = newJSONFormat(ins.encoding)
		out.Compression = newJSONFormat(ins.compression)
		if ins.decodedSize >= 0 {
			out.DecodedSize = new(ins.decodedSize)
		}
		out.Value, out.Text, out.Hint = ins.value, ins.text, ins.hint
		if ins.result.err != nil {
			out.Error = ins.result.err.Error()
		}
	}

	data, err := json.Marshal(out, jsonOptions...)
	if err != nil {
		return err
	}
	_, err = j.w.Write(append(data, '\n'))
	return err
}

func newJSONFormat(f format) *jsonFormat {
	out := &jsonFormat{Name: f.name, Note: f.note}
	if f.id >= 0 {
		out.ID = new(f.id)
	}
	return out
}
