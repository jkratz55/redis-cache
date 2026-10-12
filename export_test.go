package cache

const (
	// HeaderSizeForTest exposes headerSize to external test packages.
	HeaderSizeForTest = headerSize
	// FormatMagicForTest exposes formatMagic to external test packages.
	FormatMagicForTest = formatMagic
	// FormatVersionForTest exposes formatVersion to external test packages.
	FormatVersionForTest = formatVersion
)

// FormatsForTest exposes the formats registry to external test packages.
type FormatsForTest struct {
	f *formats
}

// NewFormatsForTest exposes newFormats to external test packages. Types declared
// in external test packages aren't treated as built-in implementations.
func NewFormatsForTest(s Serializer, c CompressionCodec, extraS []Serializer, extraC []CompressionCodec) *FormatsForTest {
	return &FormatsForTest{f: newFormats(s, c, extraS, extraC)}
}

// Encode exposes formats.encode to external test packages.
func (f *FormatsForTest) Encode(v any) ([]byte, error) {
	return f.f.encode(v)
}

// Decode exposes formats.decode to external test packages.
func (f *FormatsForTest) Decode(data []byte, dst any) error {
	return f.f.decode(data, dst)
}

// NopCodecForTest exposes nopCodec to external test packages.
func NopCodecForTest() CompressionCodec {
	return nopCodec{}
}
