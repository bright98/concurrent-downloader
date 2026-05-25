package main

import (
	"flag"
	"fmt"
	"github.com/bright98/concurrent-downloader/domain"
	"github.com/bright98/concurrent-downloader/downloader"
	"net/url"
	"os"
	"path/filepath"
)

func main() {
	inputUrl := flag.String("url", "", "URL to download (required)")
	output := flag.String("o", "", "Output file path (default: filename from URL)")
	chunkMB := flag.Float64("chunk", 2.0, "Chunk size in MB")
	flag.Parse()

	if *inputUrl == "" {
		fmt.Println("please provide a valid URL")
		os.Exit(1)
	}

	out, err := handleOutput(*inputUrl, *output)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cfg := &domain.Config{
		URL:        *inputUrl,
		OutputPath: out,
		ChunkSize:  int64(*chunkMB * 1024 * 1024),
	}

	err = downloader.Download(cfg)
	if err != nil {
		fmt.Println(err)
		fmt.Printf("err: %v \n", err)
		os.Exit(1)
	}
}

func handleOutput(inputUrl, output string) (string, error) {
	if output != "" {
		return output, nil
	}

	parsed, err := url.Parse(inputUrl)
	if err != nil {
		return "", fmt.Errorf("invalid url: %w", err)
	}
	if parsed == nil {
		return "", fmt.Errorf("invalid url")
	}

	name := filepath.Base(parsed.Path)
	if name == "" {
		output = "output"
	}

	return filepath.Join(".", name), nil
}
