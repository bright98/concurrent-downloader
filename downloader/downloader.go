package downloader

import (
	"fmt"
	"github.com/bright98/concurrent-downloader/domain"
	"net/http"
	"os"
	"path/filepath"
)

func headRequest(url string) (int64, bool, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	size := resp.ContentLength
	rangeSupported := false
	if resp.Header.Get("Accept-Ranges") == "bytes" {
		rangeSupported = true
	}
	return size, rangeSupported, err
}

func buildChunk(size, chunkSize int64) []*domain.Chunk {
	var chunks []*domain.Chunk
	start := int64(0)
	for i := 0; start < size; i++ {
		end := start + chunkSize - 1
		if end >= size {
			end = size - 1
		}
		chunks = append(chunks, &domain.Chunk{
			Index:    i,
			Start:    start,
			End:      end,
			TempFile: generateTempFileName(i),
		})
		start = end + 1
	}
	return chunks
}

func generateTempFileName(index int) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("concurrent_downloader_%d", index))
}
