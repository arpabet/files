package files

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.arpabet.com/files/config"
	"go.arpabet.com/files/files_api"
	"io"
	"os"
	"strings"
)

type protoStreamReader struct {
	fd   io.Reader
	fr   *bufio.Reader
	gzr   *gzip.Reader
	r     io.Reader
	lenBuf  [4]byte
}

func ProtoStream(r io.Reader, gzipEnabled bool) (files_api.ProtoReader, error) {

	var err error
	t := &protoStreamReader{
		fd: r,
	}

	t.fr = bufio.NewReaderSize(t.fd, config.FileRWBlockSize)

	if gzipEnabled {
		t.gzr, err = gzip.NewReader(t.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error  %v", err)
		}
		t.r = t.gzr
	} else {
		t.r = t.fr
	}

	return t, nil

}

func (t *protoStreamReader) Close() error {
	if t.gzr != nil {
		t.gzr.Close()
	}
	return nil
}

func (t *protoStreamReader) ReadTo(message proto.Message) error {

	lenBuf := t.lenBuf[:]

	n, err := io.ReadFull(t.r, lenBuf)
	if err != nil {
		return err
	} else if n != len(lenBuf) {
		return errors.Errorf("wrong number read %d, expected %d", n, len(lenBuf))
	}

	blockLen := int(binary.BigEndian.Uint32(lenBuf))

	block := make([]byte, blockLen)
	n, err = io.ReadFull(t.r, block)
	if err != nil {
		return err
	} else if n != len(block) {
		return errors.Errorf("wrong read bytes %d expected %d", n, len(block))
	}

	return proto.Unmarshal(block, message)
}

type protoFileReader struct {
	fd   *os.File
	fr   *bufio.Reader
	gzr   *gzip.Reader
	r     io.Reader
	lenBuf  [4]byte
}

func OpenProtoFile(filePath string) (files_api.ProtoReader, error) {

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Errorf("file open error '%s', %v", filePath, err)
	}

	return ProtoFile(fd)
}

func ProtoFile(fd *os.File) (files_api.ProtoReader, error) {

	var err error
	t := &protoFileReader{
		fd: fd,
	}

	t.fr = bufio.NewReaderSize(t.fd, config.FileRWBlockSize)

	if strings.HasSuffix(fd.Name(), ".gz") {
		t.gzr, err = gzip.NewReader(t.fr)
		if err != nil {
			return nil, errors.Errorf("gzip read error in '%s', %v", fd.Name(), err)
		}
		t.r = t.gzr
	} else {
		t.r = t.fr
	}

	return t, nil

}

func (t *protoFileReader) Close() error {
	if t.gzr != nil {
		t.gzr.Close()
	}
	return t.fd.Close()
}

func (t *protoFileReader) ReadTo(message proto.Message) error {

	lenBuf := t.lenBuf[:]

	n, err := io.ReadFull(t.r, lenBuf)
	if err != nil {
		return err
	} else if n != len(lenBuf) {
		return errors.Errorf("wrong number read %d, expected %d", n, len(lenBuf))
	}

	blockLen := int(binary.BigEndian.Uint32(lenBuf))

	block := make([]byte, blockLen)
	n, err = io.ReadFull(t.r, block)
	if err != nil {
		return err
	} else if n != len(block) {
		return errors.Errorf("wrong read bytes %d expected %d", n, len(block))
	}

	return proto.Unmarshal(block, message)
}

type protoStreamWriter struct {
	fd   io.Writer
	fw   *bufio.Writer
	gzw  *gzip.Writer
	bw   *bufio.Writer
	w    io.Writer
}

func NewProtoStream(fd io.Writer, gzipEnabled bool) files_api.ProtoWriter {

	t := &protoStreamWriter{
		fd:              fd,
	}

	t.fw = bufio.NewWriterSize(fd, config.FileRWBlockSize)

	if gzipEnabled {
		t.gzw = gzip.NewWriter(t.fw)
		t.bw = bufio.NewWriterSize(t.gzw, config.FileRWBlockSize)
		t.w = t.bw
	} else {
		t.w = t.fw
	}

	return t
}

func (t *protoStreamWriter) Close() (err error) {
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

func (t *protoStreamWriter) Write(message proto.Message) ([]byte, error) {
	return ProtobufWrite(t.w, message)
}

func ProtobufWrite(w io.Writer, message proto.Message) ([]byte, error) {

	var lenBufArr  [4]byte
	lenBuf := lenBufArr[:]

	blob, err := proto.Marshal(message)
	if err != nil {
		return nil, errors.Errorf("proto marshal error, %v", err)
	}

	binary.BigEndian.PutUint32(lenBuf, uint32(len(blob)))

	if n, err := w.Write(lenBuf); err != nil {
		return blob, err
	} else if n != len(lenBuf) {
		return blob, errors.Errorf("wrong number written %d, expected %d", n, len(lenBuf))
	}

	if n, err := w.Write(blob); err != nil {
		return blob, err
	} else if n != len(blob) {
		return blob, errors.Errorf("wrong number written %d, expected %d", n, len(blob))
	}

	return blob, nil
}

type protoBufWriter struct {
	fw   bytes.Buffer
	gzw  *gzip.Writer
	bw   *bufio.Writer
	w    io.Writer
}

func NewProtoBuf(gzipEnabled bool) (files_api.ProtoWriter, error) {

	t := new(protoBufWriter)

	if gzipEnabled {
		t.gzw = gzip.NewWriter(&t.fw)
		t.bw = bufio.NewWriterSize(t.gzw, config.FileRWBlockSize)
		t.w = t.bw
	} else {
		t.w = &t.fw
	}

	return t, nil
}

func (t *protoBufWriter) Close() error {
	if t.bw != nil {
		t.bw.Flush()
	}
	if t.gzw != nil {
		t.gzw.Flush()
		t.gzw.Close()
	}
	return nil
}

func (t *protoBufWriter) Buffer() io.Reader {
	return &t.fw
}

func (t *protoBufWriter) Bytes() []byte {
	return t.fw.Bytes()
}

func (t *protoBufWriter) Write(message proto.Message) ([]byte, error) {
	return ProtobufWrite(t.w, message)
}

type protoFileWriter struct {
	fd   *os.File
	fw   *bufio.Writer
	gzw  *gzip.Writer
	bw   *bufio.Writer
	w    io.Writer
}

func NewProtoFile(filePath string) (files_api.ProtoWriter, error) {

	var err error
	t := new(protoFileWriter)

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

func (t *protoFileWriter) Close() error {
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

func (t *protoFileWriter) Write(message proto.Message) ([]byte, error) {
	return ProtobufWrite(t.w, message)
}

func SplitProtoFile(inputFilePath string, holder proto.Message, limit int, partFn func (int) string) ([]string, error) {

	reader, err := OpenProtoFile(inputFilePath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var parts []string
	var writer files_api.ProtoWriter

	partNum := 1
	for cnt := limit; err == nil; cnt++ {

		err = reader.ReadTo(holder)
		if err != nil {
			break
		}

		if cnt == limit {
			if writer != nil {
				writer.Close()
				writer = nil
			}
			partFilePath := partFn(partNum)
			writer, err = NewProtoFile(partFilePath)
			if err != nil {
				break
			}
			parts = append(parts, partFilePath)
			cnt = 0
			partNum++
		}

		_, err = writer.Write(holder)
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

func JoinProtoFiles(outputFilePath string, row proto.Message, parts []string) error {

	writer, err := NewProtoFile(outputFilePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for _, part := range parts {

		reader, err := OpenProtoFile(part)
		if err != nil {
			return errors.Errorf("can not open file '%s', %v", part, err)
		}

		for {

			err = reader.ReadTo(row)
			if err != nil {
				break
			}

			_, err = writer.Write(row)
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

