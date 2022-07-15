/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package files

import (
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/protobuf/encoding/protojson"
)

var FileRWBlockSize = 1024 * 64  // 64kb

var Marshaler = &runtime.JSONPb {
	MarshalOptions: protojson.MarshalOptions{
		UseProtoNames:     true,
		EmitUnpopulated:   true,
	},
	UnmarshalOptions: protojson.UnmarshalOptions {
		DiscardUnknown: true,
	},
}
