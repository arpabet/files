package files

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"github.com/pkg/errors"
	"go.arpabet.com/files/config"
	"go.arpabet.com/files/files_api"
	"io"
	"os"
	"strings"
)

type jsonStreamWriter struct {
	fd    io.Writer
	fw    *bufio.Writer
	gzw   *gzip.Writer
	bw    *bufio.Writer
	w     io.Writer
}

func NewJsonStream(fd io.Writer, gzipEnabled bool) files_api.JsonWriter {

	t := &jsonStreamWriter{
		fd:              fd,
	}

	t.fw = bufio.NewWriterSize(t.fd, config.FileRWBlockSize)

	if gzipEnabled {
		t.gzw = gzip.NewWriter(t.fw)
		t.bw = bufio.NewWriterSize(t.gzw, config.FileRWBlockSize)
		t.w = t.bw
	} else {
		t.w = t.fw
	}

	return t
}

func (t *jsonStreamWriter) Close() (err error) {
	if t.bw != nil {
		t.bw.Flush()
	}
	if t.gzw != nil {
		t.gzw.Flush()
		err = t.gzw.Close()
	}
	t.fw.Flush()
	return err
}

func (t *jsonStreamWriter) WriteRaw(message json.RawMessage) error {
	_, err := t.w.Write(append(message, '\n'))
	return err
}

func (t *jsonStreamWriter) Write(object interface{}) error {
	return JsonWrite(t.w, object)
}

type jsonFileWriter struct {
	fd   *os.File
	fw   *bufio.Writer
	gzw   *gzip.Writer
	bw    *bufio.Writer
	w    io.Writer
}

func NewJsonFile(filePath string) (files_api.JsonWriter, error) {

	var err error
	t := new(jsonFileWriter)

	t.fd, err = os.Create(filePath)
	if err != nil {
		return nil, errors.Errorf("file create error '%s', %v", filePath, err)
	}

	t.fw = bufio.NewWriterSize(t.fd, config.FileRWBlockSize)

	if strings.HasSuffix(filePath, ".gz") {
		t.gzw = gzip.NewWriter(t.fw)
		t.bw = bufio.NewWriterSize(t.gzw, config.FileRWBlockSize)
		t.w = t.bw
	} else {
		t.w = t.fw
	}

	return t, nil
}

func (t *jsonFileWriter) Close() error {
	if t.bw != nil {
		t.bw.Flush()
	}
	if t.gzw != nil {
		t.gzw.Flush()
		t.gzw.Close()
	}
	t.fw.Flush()
	return t.fd.Close()
}

func (t *jsonFileWriter) WriteRaw(message json.RawMessage) error {
	_, err := t.w.Write(append(message, '\n'))
	return err
}

func (t *jsonFileWriter) Write(object interface{}) error {
	return JsonWrite(t.w, object)
}

func JsonWrite(w io.Writer, object interface{}) error {

	var jsonBin []byte
	jsonBin, err := config.Marshaler.Marshal(object)
	if err != nil {
		return err
	}

	_, err = w.Write(append(jsonBin, '\n'))
	return err
}

type jsonStreamReader struct {
	fr   io.Reader
	gzr   *gzip.Reader
	r     *bufio.Reader
	lastErr error
}

func JsonStream(fr io.Reader, gzipEnabled bool) (files_api.JsonReader, error) {

	var err error
	t := &jsonStreamReader{
		fr: fr,
	}

	if gzipEnabled {
		t.gzr, err = gzip.NewReader(t.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error, %v", err)
		}
		t.r = bufio.NewReader(t.gzr)
	} else {
		t.r = bufio.NewReader(t.fr)
	}

	return t, nil

}

func (t *jsonStreamReader) Close() (err error) {
	if t.gzr != nil {
		err = t.gzr.Close()
	}
	return err
}

func (t *jsonStreamReader) ReadRaw() (json.RawMessage, error) {
	if t.lastErr != nil {
		return nil, t.lastErr
	}
	jsonBin, err := t.r.ReadBytes('\n')
	if len(jsonBin) > 0 {
		if err == nil {
			jsonBin = jsonBin[:len(jsonBin)-1]  // remove last '\n'
		} else if err == io.EOF {
			t.lastErr, err = err, nil
		}
	}
	return jsonBin, err
}

func (t *jsonStreamReader) Read(holder interface{}) error {
	if t.lastErr != nil {
		return t.lastErr
	}
	jsonBin, err := t.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(jsonBin) > 0 {
			// last item
			t.lastErr, err = err, nil
		} else {
			return err
		}
	}
	return config.Marshaler.Unmarshal(jsonBin, holder)
}

type jsonFileReader struct {
	fd   *os.File
	fr   *bufio.Reader
	gzr   *gzip.Reader
	r     *bufio.Reader
	lastErr error
}

func OpenJsonFile(filePath string) (files_api.JsonReader, error) {

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Errorf("file open error '%s', %v", filePath, err)
	}

	return JsonFile(fd)
}

func JsonFile(fd *os.File) (files_api.JsonReader, error) {

	var err error
	t := &jsonFileReader{
		fd: fd,
	}

	t.fr = bufio.NewReaderSize(t.fd, config.FileRWBlockSize)

	if strings.HasSuffix(fd.Name(), ".gz") {
		t.gzr, err = gzip.NewReader(t.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error in '%s', %v", fd.Name(), err)
		}
		t.r = bufio.NewReader(t.gzr)
	} else {
		t.r = t.fr
	}

	return t, nil

}

func (t *jsonFileReader) Close() error {
	if t.gzr != nil {
		t.gzr.Close()
	}
	return t.fd.Close()
}

func (t *jsonFileReader) ReadRaw() (json.RawMessage, error) {
	if t.lastErr != nil {
		return nil, t.lastErr
	}
	jsonBin, err := t.r.ReadBytes('\n')
	if len(jsonBin) > 0 {
		if err == nil {
			jsonBin = jsonBin[:len(jsonBin)-1]  // remove last '\n'
		} else if err == io.EOF {
			t.lastErr, err = err, nil
		}
	}
	return jsonBin, err
}

func (t *jsonFileReader) Read(holder interface{}) error {
	if t.lastErr != nil {
		return t.lastErr
	}
	jsonBin, err := t.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(jsonBin) > 0 {
			// last item
			t.lastErr, err = err, nil
		} else {
			return err
		}
	}
	return config.Marshaler.Unmarshal(jsonBin, holder)
}

func SplitJsonFile(inputFilePath string, limit int, partFn func (int) string) ([]string, error) {

	reader, err := OpenJsonFile(inputFilePath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var parts []string
	var writer files_api.JsonWriter

	partNum := 1
	for cnt := limit; err == nil; cnt++ {

		raw, err := reader.ReadRaw()
		if err != nil {
			break
		}

		if cnt == limit {
			if writer != nil {
				writer.Close()
				writer = nil
			}
			partFilePath := partFn(partNum)
			writer, err = NewJsonFile(partFilePath)
			if err != nil {
				break
			}
			parts = append(parts, partFilePath)
			cnt = 0
			partNum++
		}

		err = writer.WriteRaw(raw)
	}

	if err == io.EOF {
		err = nil
	}

	if writer != nil {
		writer.Close()
	}

	if err != nil {
		for _, part := range parts {
			os.Remove(part)
		}
		parts = nil
	}

	return parts, err
}

func JoinJsonFiles(outputFilePath string, parts []string) error {

	writer, err := NewJsonFile(outputFilePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for _, part := range parts {

		reader, err := OpenJsonFile(part)
		if err != nil {
			return errors.Errorf("can not open file '%s', %v", part, err)
		}

		for {

			raw, err := reader.ReadRaw()
			if err != nil {
				break
			}

			err = writer.WriteRaw(raw)
			if err != nil {
				reader.Close()
				return errors.Errorf("can not write row to file '%s', %v", outputFilePath, err)
			}

		}

		if err == io.EOF {
			err = nil
		}

		reader.Close()

		if err != nil {
			return errors.Errorf("join read file '%s', %v", part, err)
		}

	}

	return nil
}

