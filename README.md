# Concurrent Downloader

It splits files into chunks and downloads them in parallel using HTTP Range requests, then assembles them into the final file.

This project is for Bernoly interview.

## How It Works

```
1. HEAD request    → get file size, check Range support
2. Split file      → divide into N equal chunks
3. Download        → fetch all chunks concurrently (with retry)
4. Assemble        → join chunks in order into final file
```

## Installation

```bash
git clone https://github.com/bright98/concurrent-downloader
cd concurrent-downloader
go mod tidy
```

## Usage

```bash
go run main.go -url <url> [options]
```

### Flags

| Flag | Default | Description                 |
|---|---|-----------------------------|
| `-url` | required | URL of the file to download |
| `-o` | filename from URL | Output file name     |
| `-chunk` | `2.0` | Chunk size in MB            |

### Examples

```bash
# basic download
go run main.go -url https://proof.ovh.net/files/10Mb.dat

# custom output path
go run main.go -url https://proof.ovh.net/files/10Mb.dat -o file.dat

# more workers, smaller chunks
go run main.go -url https://proof.ovh.net/files/100Mb.dat -chunk 20
```

## Running Tests

```bash
go test ./... -v
```

Tests use `httptest.NewServer` to mock HTTP responses.

## Dependencies

- [`github.com/vbauerster/mpb`](https://github.com/vbauerster/mpb) — concurrent progress bars
