/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
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
	"testing"
)

func TestProtoWriteAndRead(t *testing.T) {

	fd, err := ioutil.TempFile(os.TempDir(), "proto-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	// Test Plain
	filePath = filePath + ".pb"
	writeProto(t, filePath)
	var buf bytes.Buffer
	writeProtoStream(t, files.NewProtoStream(&buf, false))

	content, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, buf.Bytes(), content)

	stream, err := files.ProtoStream(bytes.NewReader(content), false)
	require.NoError(t, err)
	readProtoStream(t, stream)
	readProto(t, filePath)

	os.Remove(filePath)

	// Test GZIP
	filePath = filePath + ".gz"
	writeProto(t, filePath)
	readProto(t, filePath)
	os.Remove(filePath)

}

func writeProto(t *testing.T, filePath string) {

	pf, err := files.NewProtoFile(filePath)
	require.NoError(t, err)

	writeProtoStream(t, pf)
}

func writeProtoStream(t *testing.T, pf files.ProtoWriter) {

	obj1 := &Domain{
		Domain:                 "obj1",
	}

	obj2 := &Domain{
		Domain:                 "obj2",
	}

	_, err := pf.Write(obj1)
	require.NoError(t, err)

	_, err = pf.Write(obj2)
	require.NoError(t, err)

	err = pf.Close()
	require.NoError(t, err)
}

func readProto(t *testing.T, filePath string) {

	reader, err := files.OpenProtoFile(filePath)
	require.NoError(t, err)

	readProtoStream(t, reader)

}

func readProtoStream(t *testing.T, reader files.ProtoReader) {

	var obj1 Domain

	err := reader.ReadTo(&obj1)
	require.NoError(t, err)

	require.Equal(t, "obj1", obj1.Domain)

	var obj2 Domain

	err = reader.ReadTo(&obj2)
	require.NoError(t, err)

	require.Equal(t, "obj2", obj2.Domain)

	err = reader.ReadTo(&obj2)
	require.Equal(t, err, io.EOF)
}


func TestProtoSplit(t *testing.T) {

	fd, err := ioutil.TempFile(os.TempDir(), "proto-test")
	require.NoError(t, err)
	filePath := fd.Name()
	fd.Close()
	os.Remove(filePath)

	protoFilePath := filePath + ".pb"

	pf, err := files.NewProtoFile(protoFilePath)
	require.NoError(t, err)

	obj1 := &Domain{
		Domain:                 "obj1",
	}

	for i := 0; i < 100; i++ {
		_, err = pf.Write(obj1)
		require.NoError(t, err)
	}

	err = pf.Close()
	require.NoError(t, err)

	parts, err := files.SplitProtoFile(protoFilePath, obj1, 10, func(i int) string {
		return fmt.Sprintf("%s_part%d.pb", filePath, i)
	})
	require.NoError(t, err)

	println(protoFilePath)
	all, err := ioutil.ReadFile(protoFilePath)
	require.NoError(t, err)
	//println(string(all))

	err = files.JoinProtoFiles(protoFilePath, obj1, parts)
	require.NoError(t, err)

	joined, err := ioutil.ReadFile(protoFilePath)
	require.NoError(t, err)

	require.Equal(t, all, joined)

	os.Remove(protoFilePath)
	for _, part := range parts {
		println("RemoveFile: ", part)
		os.Remove(part)
	}
}
