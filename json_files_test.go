package files_test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.arpabet.com/files"
	"go.arpabet.com/files/files_api"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestJsonWriteAndRead(t *testing.T) {

	fd, err := ioutil.TempFile(os.TempDir(), "json-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	// Test Plain
	filePath = filePath + ".json"
	writeJson(t, filePath)
	var buf bytes.Buffer
	writeJsonStream(t, files.NewJsonStream(&buf, false))

	content, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, buf.Bytes(), content)

	stream, err := files.JsonStream(bytes.NewReader(content), false)
	require.NoError(t, err)
	readJsonStream(t, stream)
	readJson(t, filePath)

	os.Remove(filePath)

	// Test GZIP
	filePath = filePath + ".gz"
	writeJson(t, filePath)
	readJson(t, filePath)
	os.Remove(filePath)

}

func writeJson(t *testing.T, filePath string) {

	js, err := files.NewJsonFile(filePath)
	require.NoError(t, err)

	writeJsonStream(t, js)
}

func writeJsonStream(t *testing.T, js files_api.JsonWriter) {

	obj1 := map[string]string {
		"test": "obj1",
	}

	obj2 := map[string]string {
		"test": "obj2",
	}

	err := js.Write(obj1)
	require.NoError(t, err)

	err = js.Write(obj2)
	require.NoError(t, err)

	err = js.Close()
	require.NoError(t, err)
}


func readJson(t *testing.T, filePath string) {

	reader, err := files.OpenJsonFile(filePath)
	require.NoError(t, err)

	readJsonStream(t, reader)
}

func readJsonStream(t *testing.T, reader files_api.JsonReader) {

	obj1 := make(map[string]interface{})

	err := reader.Read(&obj1)
	require.NoError(t, err)

	require.Equal(t, 1, len(obj1))
	require.Equal(t, "obj1", obj1["test"])

	obj2 := make(map[string]interface{})

	err = reader.Read(&obj2)
	require.NoError(t, err)

	require.Equal(t, 1, len(obj2))
	require.Equal(t, "obj2", obj2["test"])

	err = reader.Read(&obj2)
	require.Equal(t, err, io.EOF)

	err = reader.Close()
	require.NoError(t, err)
}

func TestJsonSplit(t *testing.T) {

	fd, err := ioutil.TempFile(os.TempDir(), "json-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	jsonFilePath := filePath + ".json"

	jf, err := files.NewJsonFile(jsonFilePath)
	require.NoError(t, err)

	obj1 := map[string]string {
		"test": "obj1",
	}

	for i := 0; i < 100; i++ {
		err = jf.Write(obj1)
		require.NoError(t, err)
	}

	err = jf.Close()
	require.NoError(t, err)

	parts, err := files.SplitJsonFile(jsonFilePath, 10, func(i int) string {
		return fmt.Sprintf("%s_part%d.json", filePath, i)
	})
	require.NoError(t, err)

	println(jsonFilePath)
	all, err := ioutil.ReadFile(jsonFilePath)
	require.NoError(t, err)
	//println(string(all))

	err = files.JoinJsonFiles(jsonFilePath, parts)
	require.NoError(t, err)

	joined, err := ioutil.ReadFile(jsonFilePath)
	require.NoError(t, err)

	require.Equal(t, string(all), string(joined))

	os.Remove(jsonFilePath)
	for _, part := range parts {
		println("RemoveFile: ", part)
		os.Remove(part)
	}
}
