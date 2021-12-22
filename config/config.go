package config

import (
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

var FileRWBlockSize = 1024 * 64  // 64kb

var Marshaler = &runtime.JSONPb {
	OrigName: true,
	EmitDefaults: true,
}
