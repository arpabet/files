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
	"go.arpabet.com/files/files_api"
	"google.golang.org/grpc/profiling/proto"
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

func writeProtoStream(t *testing.T, pf files_api.ProtoWriter) {

	obj1 := &proto.Stat{
		Tags:                 "obj1",
	}

	obj2 := &proto.Stat{
		Tags:                 "obj2",
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

func readProtoStream(t *testing.T, reader files_api.ProtoReader) {

	var obj1 proto.Stat

	err := reader.ReadTo(&obj1)
	require.NoError(t, err)

	require.Equal(t, "obj1", obj1.Tags)

	var obj2 proto.Stat

	err = reader.ReadTo(&obj2)
	require.NoError(t, err)

	require.Equal(t, "obj2", obj2.Tags)

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

	obj1 := &proto.Stat{
		Tags:                 "obj1",
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
