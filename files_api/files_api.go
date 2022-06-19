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