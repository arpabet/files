/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package files

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
)

type csvStreamWriter struct {
	fw   io.Writer
	gzw   *gzip.Writer
	csvw  *csv.Writer
	valueProcessors []CsvValueProcessor
}

func NewCsvStream(fw io.Writer, gzipEnabled bool, valueProcessors ...CsvValueProcessor) CsvWriter {

	t := &csvStreamWriter{
		fw:              fw,
		valueProcessors: valueProcessors,
	}

	if gzipEnabled {
		t.gzw = gzip.NewWriter(t.fw)
		t.csvw = csv.NewWriter(t.gzw)
	} else {
		t.csvw = csv.NewWriter(t.fw)
	}

	return t
}

func (t *csvStreamWriter) Close() (err error) {
	t.csvw.Flush()
	if t.gzw != nil {
		t.gzw.Flush()
		err = t.gzw.Close()
	}
	return err
}

func (t *csvStreamWriter) Write(values ...string) error {
	if t.valueProcessors != nil {
		return t.csvw.Write(zipValues(t.valueProcessors, values))
	} else {
		return t.csvw.Write(values)
	}
}

type csvFileWriter struct {
	fd   *os.File
	fw   *bufio.Writer
	gzw   *gzip.Writer
	csvw  *csv.Writer
	valueProcessors []CsvValueProcessor
}

func NewCsvFile(filePath string, valueProcessors ...CsvValueProcessor) (CsvWriter, error) {

	var err error
	t := new(csvFileWriter)
	t.valueProcessors = valueProcessors

	t.fd, err = os.Create(filePath)
	if err != nil {
		return nil, errors.Errorf("file create error '%s', %v", filePath, err)
	}

	t.fw = bufio.NewWriterSize(t.fd, FileRWBlockSize)

	if strings.HasSuffix(filePath, ".gz") {
		t.gzw = gzip.NewWriter(t.fw)
		t.csvw = csv.NewWriter(t.gzw)
	} else {
		t.csvw = csv.NewWriter(t.fw)
	}

	return t, nil
}

func (t *csvFileWriter) Close() error {
	t.csvw.Flush()
	if t.gzw != nil {
		t.gzw.Flush()
		t.gzw.Close()
	}
	t.fw.Flush()
	return t.fd.Close()
}

func (t *csvFileWriter) Write(values ...string) error {
	if t.valueProcessors != nil {
		return t.csvw.Write(zipValues(t.valueProcessors, values))
	} else {
		return t.csvw.Write(values)
	}
}

func zipValues(processors []CsvValueProcessor, list []string) []string {
	arr := make([]string, 0, len(list))
	for _, v := range list {
		for _, p := range processors {
			v = p(v)
		}
		arr = append(arr, v)
	}
	return arr
}

type csvStreamReader struct {
	fr   io.Reader
	gzr   *gzip.Reader
	csvr  *csv.Reader
	valueProcessors []CsvValueProcessor
}

func OpenCsvStream(fr io.Reader, gzipEnabled bool, valueProcessors ...CsvValueProcessor) (CsvStream, error) {

	var err error
	t := &csvStreamReader{
		fr: fr,
		valueProcessors: valueProcessors,
	}

	if gzipEnabled {
		t.gzr, err = gzip.NewReader(t.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error, %v", err)
		}
		t.csvr = csv.NewReader(t.gzr)
	} else {
		t.csvr = csv.NewReader(t.fr)
	}

	return t, nil

}

func (t *csvStreamReader) Close() (err error) {
	if t.gzr != nil {
		err = t.gzr.Close()
	}
	return err
}

func (t *csvStreamReader) Read() ([]string, error) {
	record, err := t.csvr.Read()
	if err != nil {
		return nil, err
	}
	if t.valueProcessors != nil {
		record = zipValues(t.valueProcessors, record)
	}
	return record, nil
}

type csvFileReader struct {
	fd   *os.File
	fr   *bufio.Reader
	gzr   *gzip.Reader
	csvr  *csv.Reader
	valueProcessors []CsvValueProcessor
}

func OpenCsvFile(filePath string, valueProcessors ...CsvValueProcessor) (CsvReader, error) {

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Errorf("file open error '%s', %v", filePath, err)
	}

	return CsvFileReader(fd, valueProcessors...)
}

func CsvFileReader(fd *os.File, valueProcessors ...CsvValueProcessor) (*csvFileReader, error) {

	var err error
	t := &csvFileReader{
		fd: fd,
		valueProcessors: valueProcessors,
	}

	t.fr = bufio.NewReaderSize(t.fd, FileRWBlockSize)

	if strings.HasSuffix(fd.Name(), ".gz") {
		t.gzr, err = gzip.NewReader(t.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error in '%s', %v", fd.Name(), err)
		}
		t.csvr = csv.NewReader(t.gzr)
	} else {
		t.csvr = csv.NewReader(t.fr)
	}

	return t, nil

}

func (t *csvFileReader) Close() error {
	if t.gzr != nil {
		t.gzr.Close()
	}
	return t.fd.Close()
}

func (t *csvFileReader) ReadHeader() (CsvFile, error) {
	header, err := t.Read()
	if err != nil {
		return nil, err
	}
	return newCsvFile(header, t), nil
}

func (t *csvFileReader) Read() ([]string, error) {
	record, err := t.csvr.Read()
	if err != nil {
		return nil, err
	}
	if t.valueProcessors != nil {
		record = zipValues(t.valueProcessors, record)
	}
	return record, nil
}

type csvFile struct {
	header []string
	index  map[string]int
	reader CsvReader
}

func newCsvFile(header []string, reader CsvReader) *csvFile {

	index := make(map[string]int)
	for i, name := range header {
		index[name] = i
	}

	return &csvFile {
		header: header,
		index: index,
		reader: reader,
	}
}

func (t *csvFile) Header() []string {
	return t.header
}

func (t *csvFile) Index() map[string]int {
	return t.index
}

func (t *csvFile) Next() (CsvRecord, error) {
	record, err := t.reader.Read()
	if err != nil {
		return nil, err
	}
	return csvRecord{record, t}, nil
}

type csvRecord struct {
	record []string
	file   *csvFile
}

func (t csvRecord) Record() []string {
	return t.record
}

func (t csvRecord) Field(name, def string) string {
	if idx, ok := t.file.index[name]; ok {
		if idx >= 0 && idx < len(t.record) {
			return t.record[idx]
		}
	}
	return def
}

func (t csvRecord) Fields() map[string]string {
	m := make(map[string]string)
	for i, val := range t.record {
		name := ""
		if i < len(t.file.header) {
			name = t.file.header[i]
		}
		m[name] = val
	}
	return m
}

type csvSchema struct {
	header []string
	index  map[string]int
}

func NewCsvSchema(header []string) CsvSchema {

	index := make(map[string]int)
	for i, name := range header {
		index[name] = i
	}

	return &csvSchema {
		header: header,
		index: index,
	}
}

func (t *csvSchema) Record(record []string) CsvRecord {
	return csvSchemaRecord {
		record,
		t,
	}
}

type csvSchemaRecord struct {
	record []string
	schema   *csvSchema
}

func (t csvSchemaRecord) Record() []string {
	return t.record
}

func (t csvSchemaRecord) Field(name, def string) string {
	if idx, ok := t.schema.index[name]; ok {
		if idx >= 0 && idx < len(t.record) {
			return t.record[idx]
		}
	}
	return def
}

func (t csvSchemaRecord) Fields() map[string]string {
	m := make(map[string]string)
	for i, val := range t.record {
		name := ""
		if i < len(t.schema.header) {
			name = t.schema.header[i]
		}
		m[name] = val
	}
	return m
}

func SplitCsvFile(inputFilePath string, limit int, partFn func (int) string) ([]string, error) {

	reader, err := OpenCsvFile(inputFilePath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var parts []string
	var writer CsvWriter

	partNum := 1
	for cnt := limit; err == nil; cnt++ {

		row, err := reader.Read()
		if err != nil {
			break
		}

		if cnt == limit {
			if writer != nil {
				writer.Close()
				writer = nil
			}
			partFilePath := partFn(partNum)
			writer, err = NewCsvFile(partFilePath)
			if err != nil {
				break
			}
			parts = append(parts, partFilePath)
			err = writer.Write(header...)
			if err != nil {
				break
			}
			cnt = 0
			partNum++
		}

		err = writer.Write(row...)
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

func JoinCsvFiles(outputFilePath string, parts []string) error {

	writer, err := NewCsvFile(outputFilePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for i, part := range parts {

		reader, err := OpenCsvFile(part)
		if err != nil {
			return errors.Errorf("can not open file '%s', %v", part, err)
		}

		header, err := reader.Read()
		if err != nil {
			reader.Close()
			return errors.Errorf("can not read header in file '%s', %v", part, err)
		}

		if i == 0 {
			err = writer.Write(header...)
			if err != nil {
				reader.Close()
				return errors.Errorf("can not write header to file '%s', %v", outputFilePath, err)
			}
		}

		for {

			row, err := reader.Read()
			if err != nil {
				break
			}

			err = writer.Write(row...)
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
