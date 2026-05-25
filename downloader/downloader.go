package downloader

import (
	"fmt"
	"github.com/bright98/concurrent-downloader/domain"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

func Download(cfg *domain.Config) error {
	// 1. get file size from HEAD
	size, rangeSupport, err := headRequest(cfg.URL)
	if err != nil {
		// TODO: handle with name of function
		return err
	}

	// 2. if url doesn't support range, download without chunk (simple download)
	if !rangeSupport || size == 0 {
		// TODO: handle log with: not required to use chunks
		return downloadWithoutChunk(cfg.URL, cfg.OutputPath)
	}

	// TODO: handle log with size and chunk size

	// 3. build chunks
	chunks := buildChunk(size, cfg.ChunkSize)

	// 4. download each chunk concurrent
	// TODO: add logs for start downloading
	var wg sync.WaitGroup
	errs := make(chan error, len(chunks))

	for _, chunk := range chunks {
		wg.Add(1)
		go func(c *domain.Chunk) {
			defer wg.Done()
			if err = downloadEachChunk(c, cfg.URL); err != nil {
				errs <- err
			}
		}(chunk)
	}

	wg.Wait()
	close(errs)

	// 5. handle goroutine errors
	for err := range errs {
		if err != nil {
			// TODO: handle error
			cleanUpChunks(chunks)
			return err
		}
	}

	// 6. assemble output
	// TODO: add log for start assembling
	err = assembleDownloadedChunks(chunks, cfg.OutputPath)
	if err != nil {
		// TODO: handle error
		return err
	}

	cleanUpChunks(chunks)
	// TODO: log finished with output file path
	return nil
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

func downloadEachChunk(chunk *domain.Chunk, url string) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		// TODO: handle error with chunk index
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk.Start, chunk.End))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// TODO: handle error with chunk index
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: [%s] on getting chunk %d", resp.Status, chunk.Index)
	}

	out, err := os.Create(chunk.TempFile)
	if err != nil {
		return err
	}
	defer out.Close()

	buf := make([]byte, 32*1024) // 32KB
	for {
		bytesRead, readErr := resp.Body.Read(buf)
		if readErr != nil {
			// TODO: handle read error with chunk index
			return err
		}
		if readErr == io.EOF {
			break
		}
		if bytesRead > 0 {
			_, writeErr := out.Write(buf[:bytesRead])
			if writeErr != nil {
				// TODO: handle write error with chunk index
				return writeErr
			}
		}
	}
	return nil
}

func cleanUpChunks(chunks []*domain.Chunk) {
	for _, chunk := range chunks {
		_ = os.Remove(chunk.TempFile)
	}
}
