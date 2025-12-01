package processors

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
)

// CSVReader wraps an io.Reader and reads it.
type CSVReader struct {
	Reader     io.Reader
	BufferSize int
	Gzipped    bool
	header     []string
	scanner    *bufio.Scanner
}

// NewCSVReader returns a new IoReader wrapping the given io.Reader object.
func NewCSVReader(filePath string) (*CSVReader, error) {
	fileReader, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &CSVReader{Reader: fileReader, BufferSize: 1024}, nil
}

// ProcessData overwrites the reader if the content is Gzipped, then defers to ForEachData
func (r *CSVReader) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	if r.Gzipped {
		gzReader, err := gzip.NewReader(r.Reader)
		etlutil.KillPipelineIfErr(err, killChan)
		r.Reader = gzReader
	}
	r.scanner = bufio.NewScanner(r.Reader)
	r.scanner.Scan()
	r.header = strings.Split(r.scanner.Text(), ",")
	r.ForEachData(killChan, func(d etldata.Payload) {
		outputChan <- d
	})
}

// Finish - see interface for documentation.
func (r *CSVReader) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

// ForEachData either reads by line or by buffered stream, sending the data
// back to the anonymous func that ultimately shoves it onto the outputChan
func (r *CSVReader) ForEachData(killChan chan error, foo func(d etldata.Payload)) {
	r.scanLines(killChan, foo)
}

func (r *CSVReader) scanLines(killChan chan error, forEach func(d etldata.Payload)) {
	values := []string{}
	mut := sync.Mutex{}
	wg := sync.WaitGroup{}
	for r.scanner.Scan() {
		wg.Add(1)
		value := r.scanner.Text()
		go func(value string) {
			valueArray := strings.Split(value, ",")
			valueString := "{"
			if len(valueArray) != len(r.header) {
				wg.Done()
				return
			}
			for i, header := range r.header {
				var insertValue string
				if valueArray[i] == "" {
					insertValue = "\"\""
				} else {
					insertValue = valueArray[i]
					insertValue = strings.ReplaceAll(insertValue, "\t", "")
					insertValue = strings.ReplaceAll(insertValue, "\\", "")
					insertValue = strings.ReplaceAll(insertValue, "%", "")
				}
				valueString = valueString + header + ":" + insertValue
				if i != len(r.header)-1 {
					valueString = valueString + ","
				}
			}

			valueString = valueString + "}"
			mut.Lock()
			values = append(values, valueString)
			mut.Unlock()
			wg.Done()
		}(value)
	}

	wg.Wait()
	for _, value := range values {
		forEach(etldata.JSON(value))
	}
	err := r.scanner.Err()
	etlutil.KillPipelineIfErr(err, killChan)
}

func (r *CSVReader) String() string {
	return "CSVReader"
}
