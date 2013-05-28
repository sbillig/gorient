package gorient

import (
	"fmt"
//	"reflect"
	"strconv"
	"testing"
)

func marsh(t *testing.T, v interface{}, sv string) {
	out, err := Marshal(v)
	if err != nil {
		t.FailNow()
	}
	if string(out) != sv {
		fmt.Println("Marshal",v,"->",strconv.Quote(string(out)),
			" expected:",strconv.Quote(sv))
		t.Fail()
	}
}

func TestBasic(t *testing.T) {
	marsh(t, float32(4.5), "4.5f")
	marsh(t, float64(4.5), "4.5d")
	marsh(t, byte(64), "64b")
	marsh(t, int16(120), "120s")
	marsh(t, int32(120), "120")
	marsh(t, int64(120), "120l")
	marsh(t, "hello", "\"hello\"")
}

func TestMap(t *testing.T) {
	m := make(map[string]interface{}, 8)
	m["name"] = "Bob"
	marsh(t, m, `{"name":"Bob"}`)

	m["age"] = int16(32)
	marsh(t, m, `{"age":32s,"name":"Bob"}`)

	m["spouse"] = "Pat"
	marsh(t, m, `{"age":32s,"name":"Bob","spouse":"Pat"}`)

}
