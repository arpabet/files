/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
*/

package files_test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.arpabet.com/files"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestCsvWriteAndRead(t *testing.T) {

	fd, err := ioutil.TempFile(os.TempDir(), "csv-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	// Test Plain
	filePath = filePath + ".csv"
	writeCsv(t, filePath)
	var buf bytes.Buffer
	writeCsvStream(t, files.NewCsvStream(&buf, false, files.PandasFriendly))

	content, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, buf.Bytes(), content)
	require.Equal(t, "123,#,#,#,#\n", string(content))
	readCsv(t, filePath)
	stream, err := files.OpenCsvStream(bytes.NewReader(content), false, strings.TrimSpace, files.RemoveHash)
	readCsvStream(t, stream)

	// Test With Header
	writeCsvWithHeader(t, filePath)
	readCsvWithHeader(t, filePath)

	os.Remove(filePath)

	// Test GZIP
	filePath = filePath + ".gz"
	writeCsv(t, filePath)
	readCsv(t, filePath)
	os.Remove(filePath)

}

func readCsv(t *testing.T, filePath string) {

	reader, err := files.OpenCsvFile(filePath, strings.TrimSpace, files.RemoveHash)
	require.NoError(t, err)

	record, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, "123,,,,", strings.Join(record, ","))

	_, err = reader.Read()
	require.Equal(t, err, io.EOF)

	err = reader.Close()
	require.NoError(t, err)
}

func readCsvStream(t *testing.T, reader files.CsvStream) {

	record, err := reader.Read()
	require.NoError(t, err)

	require.Equal(t, "123,,,,", strings.Join(record, ","))

	_, err = reader.Read()
	require.Equal(t, err, io.EOF)

	err = reader.Close()
	require.NoError(t, err)
}

func readCsvWithHeader(t *testing.T, filePath string) {

	reader, err := files.OpenCsvFile(filePath, strings.TrimSpace, files.RemoveHash)
	require.NoError(t, err)

	file, err := reader.ReadHeader()
	require.NoError(t, err)
	require.Equal(t, "name,value", strings.Join(file.Header(), ","))
	require.Equal(t, 0, file.Index()["name"])
	require.Equal(t, 1, file.Index()["value"])

	schema := files.NewCsvSchema(file.Header())

	record, err := file.Next()
	require.NoError(t, err)

	require.Equal(t, "one,1", strings.Join(record.Record(), ","))
	require.Equal(t, "one", record.Field("name", ""))
	require.Equal(t, "1", record.Field("value", ""))

	fields := record.Fields()
	require.Equal(t, 2, len(fields))
	require.Equal(t, "one", fields["name"])
	require.Equal(t, "1", fields["value"])

	require.Equal(t, schema.Record(record.Record()).Fields(), fields)

	_, err = file.Next()
	require.Equal(t, err, io.EOF)

	err = reader.Close()
	require.NoError(t, err)
}


func writeCsv(t *testing.T, filePath string) {
	csv, err := files.NewCsvFile(filePath, files.PandasFriendly)
	require.NoError(t, err)

	writeCsvStream(t, csv)
}

func writeCsvStream(t *testing.T, csv files.CsvWriter) {

	err := csv.Write(" 123 ", "", " ", "null", "NaN")
	require.NoError(t, err)

	err = csv.Close()
	require.NoError(t, err)
}

func writeCsvWithHeader(t *testing.T, filePath string) {
	csv, err := files.NewCsvFile(filePath, files.PandasFriendly)
	require.NoError(t, err)

	err = csv.Write("name ", " value ")
	require.NoError(t, err)

	err = csv.Write(" one ", " 1 ")
	require.NoError(t, err)

	err = csv.Close()
	require.NoError(t, err)
}

func TestCsvSplit(t *testing.T) {

	fd, err := ioutil.TempFile(os.TempDir(), "csv-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	csvfilePath := filePath + ".csv"

	csv, err := files.NewCsvFile(csvfilePath)
	require.NoError(t, err)

	err = csv.Write("name", "count")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = csv.Write(fmt.Sprintf("name%d", i), strconv.Itoa(i))
		require.NoError(t, err)
	}

	err = csv.Close()
	require.NoError(t, err)

	parts, err := files.SplitCsvFile(csvfilePath, 10, func(i int) string {
		return fmt.Sprintf("%s_part%d.csv", filePath, i)
	})
	require.NoError(t, err)

	println(csvfilePath)
	all, err := ioutil.ReadFile(csvfilePath)
	require.NoError(t, err)
	//println(string(all))

	err = files.JoinCsvFiles(csvfilePath, parts)
	require.NoError(t, err)

	joined, err := ioutil.ReadFile(csvfilePath)
	require.NoError(t, err)

	require.Equal(t, all, joined)

	os.Remove(csvfilePath)
	for _, part := range parts {
		println("RemoveFile: ", part)
		os.Remove(part)
	}
}
