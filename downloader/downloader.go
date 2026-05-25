package downloader

import (
	"fmt"
	"github.com/bright98/concurrent-downloader/domain"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

func download() {
	// 1. get file size from HEAD
	// 2. if url doesn't support range, download without chunk (simple download)
	// 3. build chunks
	// 4. download each file concurrent
	// 5. assemble output
}

func headRequest(url string) (int64, bool, error) {
	resp, err := http.Head(url)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	size := resp.ContentLength
	rangeSupported := false
	// download without chunk if it doesn't support
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

func assembleDownloadedChunks(chunks []*domain.Chunk, outputPath string) error {
	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, chunk := range chunks {
		f, err := os.Open(chunk.TempFile)
		if err != nil {
			// TODO: handle error with chunk index
			return err
		}
		_, err = io.Copy(out, f)
		if err != nil {
			_ = f.Close()
			// TODO: handle error with chunk index
			return err
		}
		_ = f.Close()
	}
	return nil
}

func downloadWithoutChunk(url string, outputPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
