package hrtclientproto

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"libdb.so/hrtclient"
	"libdb.so/hrtproto"
)

// ProtobufCodec is a Codec for Protobuf messages.
var ProtobufCodec hrtclient.Codec = protobufCodec{}

type protobufCodec struct{}

func (e protobufCodec) Encode(r *http.Request, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protobufCodec: Encode: %T is not a proto.Message", v)
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("protobufCodec: Encode: %w", err)
	}

	r.Header.Set("Content-Type", "application/protobuf")
	r.Header.Set(hrtproto.MessageNameHeader, string(proto.MessageName(msg)))
	requestSetBytes(r, b)

	return nil
}

func (e protobufCodec) Decode(r *http.Response, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protobufCodec: Decode: %T is not a proto.Message", v)
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	name := r.Header.Get(hrtproto.MessageNameHeader)
	if name != "" && name != string(proto.MessageName(msg)) {
		return fmt.Errorf("protobufCodec: Decode: %s is not the expected message type", name)
	}

	if err := proto.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("protobufCodec: Decode: %w", err)
	}

	return nil
}

// ProtoJSONCodec is a Codec for Protobuf messages encoded in JSON.
var ProtoJSONCodec hrtclient.Codec = protoJSONCodec{}

type protoJSONCodec struct{}

func (e protoJSONCodec) Encode(r *http.Request, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protoJSONCodec: Encode: %T is not a proto.Message", v)
	}

	b, err := protojson.Marshal(msg)
	if err != nil {
		return fmt.Errorf("protoJSONCodec: Encode: %w", err)
	}

	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(hrtproto.MessageNameHeader, string(proto.MessageName(msg)))
	requestSetBytes(r, b)

	return nil
}

func (e protoJSONCodec) Decode(r *http.Response, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protoJSONCodec: Decode: %T is not a proto.Message", v)
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	name := r.Header.Get(hrtproto.MessageNameHeader)
	if name != "" && name != string(proto.MessageName(msg)) {
		return fmt.Errorf("protoJSONCodec: Decode: %s is not the expected message type", name)
	}

	if err := protojson.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("protoJSONCodec: Decode: %w", err)
	}

	return nil
}

func requestSetBytes(r *http.Request, b []byte) {
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	r.Body, _ = r.GetBody()
	r.ContentLength = int64(len(b))
}
