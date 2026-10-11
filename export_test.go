package cache

// NewFormatsForTest exposes newFormats to external test packages. Types declared
// in external test packages aren't treated as built-in implementations.
func NewFormatsForTest(s Serializer, c CompressionCodec, extraS []Serializer, extraC []CompressionCodec) {
	newFormats(s, c, extraS, extraC)
}
