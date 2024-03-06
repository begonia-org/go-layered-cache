package golayeredbloom

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type TypeEnum int

const (
	// TypeNone is the default type
	TypeInt TypeEnum = iota
	TypeUint
	TypeInt8
	TypeUint8
	TypeInt16
	TypeUint16
	TypeInt32
	TypeUint32
	TypeInt64
	TypeUint64
	TypeFloat32
	TypeFloat64
	TypeComplex64
	TypeComplex128
	TypeNumber

	TypeString
	TypeBytes
	TypeBool
	TypeStruct
	TypeJson
)

func EncodeToBytes(src interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(src)
	if err != nil {
		return nil, fmt.Errorf("gob.Encode failed: %w", err)
	}
	return buf.Bytes(), nil

}

func DecodeFromBytes(src []byte, dst interface{}) error {
	buf := bytes.NewBuffer(src)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(dst)
	if err != nil {
		return fmt.Errorf("gob.Decode failed: %w", err)
	}
	return nil

}
