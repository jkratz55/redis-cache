package cache

func chunk[T any](slice []T, size int) [][]T {
	if size < 1 {
		panic("illegal size, cannot create chunks whose size is less than 1")
	}
	chunks := make([][]T, 0)
	for i := 0; i < len(slice); i += size {
		end := i + size
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}
