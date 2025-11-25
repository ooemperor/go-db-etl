package processors

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"

	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
)

// JSONReader wraps an io.Reader and reads it.
type JSONReader struct {
	Reader     io.Reader
	LineByLine bool // defaults to true
	BufferSize int
	Gzipped    bool
}

// NewJSONReader returns a new IoReader wrapping the given io.Reader object.
func NewJSONReader(filePath string) (*JSONReader, error) {
	fileReader, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &JSONReader{Reader: fileReader, LineByLine: true, BufferSize: 1024}, nil
}

// ProcessData overwrites the reader if the content is Gzipped, then defers to ForEachData
func (r *JSONReader) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	if r.Gzipped {
		gzReader, err := gzip.NewReader(r.Reader)
		etlutil.KillPipelineIfErr(err, killChan)
		r.Reader = gzReader
	}
	r.ForEachData(killChan, func(d etldata.Payload) {
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (r *JSONReader) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

// ForEachData either reads by line or by buffered stream, sending the data
// back to the anonymous func that ultimately shoves it onto the outputChan
func (r *JSONReader) ForEachData(killChan chan error, foo func(d etldata.Payload)) {
	if r.LineByLine {
		r.scanLines(killChan, foo)
	} else {
		r.bufferedRead(killChan, foo)
	}
}

func (r *JSONReader) scanLines(killChan chan error, forEach func(d etldata.Payload)) {
	scanner := bufio.NewScanner(r.Reader)
	for scanner.Scan() {
		forEach(etldata.JSON(scanner.Text()))
	}
	err := scanner.Err()
	etlutil.KillPipelineIfErr(err, killChan)
}

func (r *JSONReader) bufferedRead(killChan chan error, forEach func(d etldata.Payload)) {
	reader := bufio.NewReader(r.Reader)
	d := make([]byte, r.BufferSize)
	for {
		n, err := reader.Read(d)
		if err != nil && err != io.EOF {
			killChan <- err
		}
		if n == 0 {
			break
		}
		forEach(etldata.JSON(d))
		d = make([]byte, r.BufferSize)
	}
}

func (r *JSONReader) String() string {
	return "JSONReader"
}
