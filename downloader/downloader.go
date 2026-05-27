package downloader

import (
	"fmt"
	"github.com/bright98/concurrent-downloader/domain"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	maxRetry      = 3
	retryBaseWait = 1 * time.Second
)

func Download(cfg *domain.Config) error {
	// 0. initial
	client := &http.Client{}

	// 1. get file size from HEAD
	size, rangeSupport, err := headRequest(cfg.URL, client)
	if err != nil {
		return fmt.Errorf("head file err: [%w]", err)
	}

	// 2. if url doesn't support range, download without chunk (simple download)
	if !rangeSupport || size == 0 {
		fmt.Printf("file doesn't support any range. downloading the file without chunking.\n")
		return downloadWithoutChunk(cfg.URL, cfg.OutputPath, client)
	}

	// 3. build chunks
	chunks := buildChunk(size, cfg.ChunkSize)
	fmt.Printf("file size: %s | chunk size: %s | splited file in %d chunks.\n", formatBytes(size), formatBytes(cfg.ChunkSize), len(chunks))

	// 4. download each chunk concurrent
	fmt.Printf("start downloading chunks...\n")
	var wg sync.WaitGroup
	errs := make(chan error, len(chunks))

	// .5 initial progressbar
	progress := mpb.New(mpb.WithWidth(60))

	for _, chunk := range chunks {
		bar := createChunkBar(progress, chunk, len(chunks))

		wg.Add(1)
		go func(c *domain.Chunk, b *mpb.Bar) {
			defer wg.Done()
			err = withRetry(c.Index, bar, func() error {
				return downloadEachChunk(c, cfg.URL, client, b)
			})
			if err != nil {
				b.Abort(true)
				errs <- err
			}
		}(chunk, bar)
	}

	wg.Wait()
	close(errs)

	// 5. handle goroutine errors
	for err = range errs {
		if err != nil {
			cleanUpChunks(chunks)
			return fmt.Errorf("download chunk err: [%w]", err)
		}
	}

	// 6. assemble output
	fmt.Printf("finished downloading chunks. start assembling...\n")
	err = assembleDownloadedChunks(chunks, cfg.OutputPath)
	if err != nil {
		return fmt.Errorf("assembling file err: [%w]", err)
	}

	cleanUpChunks(chunks)
	fmt.Printf("finished. file saved to: %s\n", cfg.OutputPath)
	return nil
}

func headRequest(url string, client *http.Client) (int64, bool, error) {
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, false, fmt.Errorf("http status code %d", resp.StatusCode)
	}

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
			return fmt.Errorf("open file err: [%w] in chunk %d", err, chunk.Index)
		}
		_, err = io.Copy(out, f)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("copy file err: [%w] in chunk %d", err, chunk.Index)
		}
		_ = f.Close()
	}
	return nil
}

func downloadWithoutChunk(url string, outputPath string, client *http.Client) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("make request err: [%w]", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request err: [%w]", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: [%s]", resp.Status)
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func downloadEachChunk(chunk *domain.Chunk, url string, client *http.Client, bar *mpb.Bar) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("make request err: [%w] in chunk %d", err, chunk.Index)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk.Start, chunk.End))
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request err: [%w] in chunk %d", err, chunk.Index)
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

	buf := make([]byte, 32*1024)  // 32KB
	progressStarted := time.Now() // for progress bar speed

	for {
		bytesRead, readErr := resp.Body.Read(buf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read response err: [%w] in chunk %d", readErr, chunk.Index)
		}
		if bytesRead > 0 {
			_, writeErr := out.Write(buf[:bytesRead])
			if writeErr != nil {
				return fmt.Errorf("write response in tmp err: [%w] in chunk %d", writeErr, chunk.Index)
			}
		}
		// progress bar
		bar.EwmaIncrBy(bytesRead, time.Since(progressStarted))
		progressStarted = time.Now()
	}
	return nil
}

func cleanUpChunks(chunks []*domain.Chunk) {
	for _, chunk := range chunks {
		_ = os.Remove(chunk.TempFile)
	}
}

func createChunkBar(progress *mpb.Progress, chunk *domain.Chunk, chunksLen int) *mpb.Bar {
	indexWidth := len(fmt.Sprintf("%d", chunksLen))

	bar := progress.AddBar(chunk.End-chunk.Start+1,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("chunk %*d ", indexWidth, chunk.Index+1), decor.WC{W: 10}),
			decor.CountersKibiByte("% .2f / % .2f"),
		),
		mpb.AppendDecorators(
			decor.EwmaSpeed(decor.SizeB1024(0), "% .2f | ", 60),
			decor.OnComplete(decor.EwmaETA(decor.ET_STYLE_GO, 60), "done!"),
		),
	)
	return bar
}

func formatBytes(b int64) string {
	const mb = 1024 * 1024
	if b >= mb {
		return fmt.Sprintf("%.1fMB", float64(b)/mb)
	}
	return fmt.Sprintf("%.1fKB", float64(b)/1024)
}

func withRetry(chinkIndex int, bar *mpb.Bar, fn func() error) error {
	for attempt := 0; attempt < maxRetry; attempt++ {
		err := fn()
		if err != nil {
			fmt.Printf("chunck %d failed (attempt %d/%d): %v. Retrying in %v...\n",
				chinkIndex, attempt+1, maxRetry, err, retryBaseWait)
			bar.SetCurrent(0)
			time.Sleep(retryBaseWait)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed after %d retries", maxRetry)
}
