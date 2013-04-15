package main

import (
	"fmt"
	"net"
	"encoding/binary"
)

type Command byte
const (
	SHUTDOWN Command = iota + 1
	CONNECT
	DB_OPEN
	DB_CREATE
)

type Xx struct {
	conn net.Conn
	sess int32
}

func (x *Xx) read(data interface{}) {
	binary.Read(x.conn, binary.BigEndian, data)
}
func (x *Xx) write(data ...interface{}) {
	for _, d := range data {
		switch d.(type) {
		case string:
			b := []byte(d.(string))
			binary.Write(x.conn, binary.BigEndian, int32(len(b)))
			binary.Write(x.conn, binary.BigEndian, &b)
		default:
			binary.Write(x.conn, binary.BigEndian, d)
		}
	}
}
func (x *Xx) readBytes() []byte {
	var l int32
	x.read(&l)
	if l <= 0 {
		return nil
	}
	buf := make([]byte, l)
	x.read(buf)
	return buf
}
func (x *Xx) readString() string {
	return string(x.readBytes())
}
func (x *Xx) readByte() byte {
	var n byte
	x.read(&n)
	return n
}
func (x *Xx) readInt() int32 {
	var n int32
	x.read(&n)
	return n
}
func (x *Xx) beginReq(command Command) {
	x.write(command, x.sess)
}
func (x *Xx) beginResp() {
	x.readByte()
	// TODO: succ != 0 => read error
	x.readInt()
	// TODO: sess != x.sess
}

type cluster struct {
	name string
	id int16
	typ string
	segId int16
}

func (x *Xx) open(host, db, user, pass string) error {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		fmt.Println("failed to connect:",err)
		return err
	}
	x.conn = conn
	x.sess = -1

	var proto int16
	x.read(&proto)
	fmt.Println("db protocol:",proto)
	proto = 15

	x.beginReq(DB_OPEN)
	x.write("gorient", "alpha", proto, "a client id")
	x.write(db, "document", user, pass)

	x.beginResp()
	x.read(&x.sess)

	var cc int16
	x.read(&cc)
	fmt.Println("cluster count:",cc)

	cs := make([]cluster, cc)
	for i := range cs {
		c := &cs[i]
		c.name = x.readString()
		x.read(&c.id)
		c.typ = x.readString()
		x.read(&c.segId)
	}
	fmt.Println("clusters:",cs)
	fmt.Println("cconfig:",x.readBytes())
	fmt.Println("v:",x.readString())

	return nil
}

func (x *Xx) close() {
	// TODO: DB_CLOSE
	x.conn.Close()
}

func main() {
	var x Xx
	err := x.open("localhost:2424", "temp", "admin", "admin")
	if err != nil {
		fmt.Println("err:",err)
		return
	}
	defer x.close()

	fmt.Println(x)
}
