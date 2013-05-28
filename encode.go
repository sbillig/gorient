package gorient

import (
	"bytes"
	"fmt"
	"math"
	r "reflect"
	"sort"
	"strconv"
)

type encodeState struct {
	bytes.Buffer
	scratch [64]byte
}

func Marshal(v interface{}) ([]byte, error) {
	e := &encodeState{}
	err := e.marshal(v)
	if err != nil {
		return nil, err
	}
	return e.Bytes(), nil
}
func (e *encodeState) marshal(v interface{}) error {
	e.reflectValue(r.ValueOf(v))
	return nil
}

var suffix map[r.Kind]string = map[r.Kind]string {
	r.Uint8: "b",
	r.Uint16: "s",
	r.Uint64: "l",
	r.Uint: "l",
    r.Int8: "b",
    r.Int16: "s",
    r.Int64: "l",
    r.Int: "l",
    r.Float32: "f",
    r.Float64: "d",
}

func (e *encodeState) reflectValue(v r.Value) {

	k := v.Kind()
	switch k {
	case r.Bool:
		if v.Bool() {
			e.WriteString("true")
		} else {
			e.WriteString("false")
		}

	case r.Uint, r.Uint8, r.Uint16, r.Uint32, r.Uint64:
		b := strconv.AppendUint(e.scratch[:0], v.Uint(), 10)
		e.WriteString(string(b))
		e.WriteString(suffix[k])

	case r.Int, r.Int8, r.Int16, r.Int32, r.Int64:
		b := strconv.AppendInt(e.scratch[:0], v.Int(), 10)
		e.WriteString(string(b))
		e.WriteString(suffix[k])

	case r.Float32, r.Float64:
		f := v.Float()
		if math.IsInf(f, 0) || math.IsNaN(f) {
			// TODO
		}
		b := strconv.AppendFloat(e.scratch[:0], f, 'g', -1, v.Type().Bits())
		e.WriteString(string(b))
		e.WriteString(suffix[k])

	case r.String:
		e.WriteString(strconv.Quote(v.String()))

	case r.Map:
		if v.Type().Key().Kind() != r.String {
			panic("bad map type")
		}
		if v.IsNil() {
			e.WriteString("null")
			break
		}
		e.WriteByte('{')
		keys := make([]string, len(v.MapKeys()))
		for i, k := range v.MapKeys() {
			keys[i] = k.String()
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				e.WriteByte(',')
			}
			e.WriteString(strconv.Quote(k))
			e.WriteByte(':')

			// TODO: probably inefficient
			e.reflectValue(v.MapIndex(r.ValueOf(k)))
		}
		e.WriteByte('}')

	case r.Interface, r.Ptr:
		if v.IsNil() {
			e.WriteString("null")
			return
		}
		e.reflectValue(v.Elem())

	default:
		panic(fmt.Sprintf("Unrecognized Kind: %s", k))
	}
}
