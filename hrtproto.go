package hrtproto

import (
	"fmt"
	"io"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"libdb.so/hrt"
)

// MessageNameHeader is the header key used to store the message name in a response.
const MessageNameHeader = "X-Protobuf-Message-Name"

// ProtobufEncoder is an Encoder for Protobuf messages.
var ProtobufEncoder hrt.Encoder = protobufEncoder{}

type protobufEncoder struct{}

func (e protobufEncoder) Encode(w http.ResponseWriter, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protobufEncoder: Encode: %T is not a proto.Message", v)
	}

	b, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("protobufEncoder: Encode: %w", err)
	}

	w.Header().Set("Content-Type", "application/protobuf")
	w.Header().Set(MessageNameHeader, string(proto.MessageName(msg)))
	w.Write(b)
	return nil
}

func (e protobufEncoder) Decode(r *http.Request, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protobufEncoder: Decode: %T is not a proto.Message", v)
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	name := r.Header.Get(MessageNameHeader)
	if name != "" && name != string(proto.MessageName(msg)) {
		return fmt.Errorf("protobufEncoder: Decode: %s is not the expected message type", name)
	}

	if err := proto.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("protobufEncoder: Decode: %w", err)
	}

	return nil
}

// ProtoJSONEncoder is an Encoder for Protobuf messages encoded in JSON.
var ProtoJSONEncoder hrt.Encoder = protoJSONEncoder{}

type protoJSONEncoder struct{}

func (e protoJSONEncoder) Encode(w http.ResponseWriter, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protoJSONEncoder: Encode: %T is not a proto.Message", v)
	}

	b, err := protojson.Marshal(msg)
	if err != nil {
		return fmt.Errorf("protoJSONEncoder: Encode: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(MessageNameHeader, string(proto.MessageName(msg)))
	w.Write(b)
	return nil
}

func (e protoJSONEncoder) Decode(r *http.Request, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protoJSONEncoder: Decode: %T is not a proto.Message", v)
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	name := r.Header.Get(MessageNameHeader)
	if name != "" && name != string(proto.MessageName(msg)) {
		return fmt.Errorf("protoJSONEncoder: Decode: %s is not the expected message type", name)
	}

	if err := protojson.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("protoJSONEncoder: Decode: %w", err)
	}

	return nil
}
