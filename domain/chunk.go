package domain

type Chunk struct {
	Index    int
	Start    int64
	End      int64
	TempFile string
}
