package files_api

import (
	"encoding/json"
	"github.com/gogo/protobuf/proto"
)


type JsonWriter interface {

	WriteRaw(message json.RawMessage) error

    Write(object interface{}) error

	Close() error

}


type JsonReader interface {

	ReadRaw() (json.RawMessage, error)

	Read(holder interface{}) error

	Close() error

}


type ProtoWriter interface {

	Write(message proto.Message) ([]byte, error)

	Close() error

}


type ProtoReader interface {

	ReadTo(message proto.Message) error

	Close() error

}


type CsvValueProcessor  func(string) string

type CsvWriter interface {

	Write(values ...string) error

	Close() error

}

type CsvStream interface {

	Read() ([]string, error)

	Close() error
}


type CsvReader interface {

	ReadHeader() (CsvFile, error)

	Read() ([]string, error)

	Close() error
}

type CsvSchema interface {

	Record(record []string) CsvRecord

}

type CsvRecord interface {

	Record() []string

	Field(name string, def string) string

	Fields() map[string]string

}

type CsvFile interface {

	Header() []string

	Index() map[string]int

	Next() (CsvRecord, error)

}