package downloader

import (
	"fmt"
	"github.com/bright98/concurrent-downloader/domain"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// headRequest
func TestHeadRequest_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	_, _, err := headRequest(srv.URL)
	if err == nil {
		t.Error("expected error for non-200 status")
	}
}
func TestHeadRequest_RangeSupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, rangeSupported, err := headRequest(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rangeSupported {
		t.Error("expected range to be supported!")
	}
}
func TestHeadRequest_RangeNotSupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, rangeSupported, err := headRequest(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rangeSupported {
		t.Error("expected range to not be supported!")
	}
}

// buildChunk
func TestBuildChunk_CorrectCount(t *testing.T) {
	chunks := buildChunk(10*1024*1024, 2*1024*1024) // 10 / 2 = 5 chunks
	if len(chunks) != 5 {
		t.Fatalf("expected 5 chunks, got %d", len(chunks))
	}
}

// generateTempFileName
func TestGenerateTempFileName_ContainsIndex(t *testing.T) {
	name := generateTempFileName(42)
	expected := fmt.Sprintf("concurrent_downloader_%d", 42)
	if !strings.Contains(name, expected) {
		t.Fatalf("expected %s, got %s", expected, name)
	}
}
func TestGenerateTempFileName_Unique(t *testing.T) {
	name0 := generateTempFileName(0)
	name1 := generateTempFileName(1)

	if name0 == name1 {
		t.Error("temp file names should be unique per index")
	}
}

// assembleDownloadedChunks
func TestAssembleDownloadedChunks_CorrectOrder(t *testing.T) {
	dir := t.TempDir()

	// create 3 temp chunk files with known content
	chunks := []*domain.Chunk{
		{Index: 0, TempFile: dir + "/chunk_0"},
		{Index: 1, TempFile: dir + "/chunk_1"},
	}

	contents := [][]byte{
		[]byte("hello "),
		[]byte("world"),
	}

	for i, c := range chunks {
		if err := os.WriteFile(c.TempFile, contents[i], 0644); err != nil {
			t.Fatalf("failed to write chunk %d: %v", i, err)
		}
	}

	outputPath := dir + "/output.bin"
	if err := assembleDownloadedChunks(chunks, outputPath); err != nil {
		t.Fatalf("assembly failed: %v", err)
	}

	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	expected := "hello world"
	if string(got) != expected {
		t.Errorf("expected %q, got %q", expected, string(got))
	}
}
func TestAssembleDownloadedChunks_MissingChunk(t *testing.T) {
	dir := t.TempDir()
	chunks := []*domain.Chunk{
		{Index: 0, TempFile: dir + "/chunk_0"},
		{Index: 1, TempFile: dir + "/chunk_1_missing"},
	}
	os.WriteFile(chunks[0].TempFile, []byte("data"), 0644)

	err := assembleDownloadedChunks(chunks, dir+"/output.bin")
	if err == nil {
		t.Error("expected error for missing chunk file")
	}
}

// downloadEachChunk
func TestDownloadEachChunk_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	chunk := &domain.Chunk{
		Index:    0,
		Start:    0,
		End:      100,
		TempFile: t.TempDir() + "/chunk_0",
	}

	err := downloadEachChunk(chunk, srv.URL, &http.Client{}, nil)
	if err == nil {
		t.Error("expected error for 403 status")
	}
}
