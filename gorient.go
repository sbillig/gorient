package gorient

import (
	"fmt"
	"net"
	"encoding/binary"
)

type Command byte

const (
	SHUTDOWN                       Command = 1
	CONNECT                                = 2
	DB_OPEN                                = 3
	DB_CREATE                              = 4
	DB_CLOSE                               = 5
	DB_EXIST                               = 6
	DB_DROP                                = 7
	DB_SIZE                                = 8
	DB_COUNTRECORDS                        = 9
	DATACLUSTER_ADD                        = 10
	DATACLUSTER_DROP                       = 11
	DATACLUSTER_COUNT                      = 12
	DATACLUSTER_DATARANGE                  = 13
	DATACLUSTER_COPY                       = 14
	DATACLUSTER_LH_CLUSTER_IS_USED         = 16
	DATASEGMENT_ADD                        = 20
	DATASEGMENT_DROP                       = 21
	RECORD_METADATA                        = 29
	RECORD_LOAD                            = 30
	RECORD_CREATE                          = 31
	RECORD_UPDATE                          = 32
	RECORD_DELETE                          = 33
	RECORD_COPY                            = 34
	RECORD_CHANGE_IDENTITY                 = 35
	POSITIONS_HIGHER                       = 36
	POSITIONS_LOWER                        = 37
	RECORD_CLEAN_OUT                       = 38
	POSITIONS_FLOOR                        = 39
	COUNT                                  = 40 // use DATACLUSTER_COUNT
	COMMAND                                = 41
	POSITIONS_CEILING                      = 42
	TX_COMMIT                              = 60
	CONFIG_GET                             = 70
	CONFIG_SET                             = 71
	CONFIG_LIST                            = 72
	DB_RELOAD                              = 73
	DB_LIST                                = 74
	PUSH_RECORD                            = 79
	PUSH_DISTRIB_CONFIG                    = 80

	// DISTRIBUTED
	DB_COPY     = 90
	REPLICATION = 91
	CLUSTER     = 92
	DB_TRANSFER = 93

	// Lock + sync
	DB_FREEZE  = 94
	DB_RELEASE = 95

	// INCOMING
	STATUS_OK    = 0
	STATUS_ERROR = 1
	PUSH_DATA    = 3

	// CONSTANTS
	RECORD_NULL              int16 = -2
	RECORD_RID               int16 = -3
	CURRENT_PROTOCOL_VERSION int16 = 15
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
func (x *Xx) readInt32() int32 {
	var n int32
	x.read(&n)
	return n
}
func (x *Xx) readInt64() int64 {
	var n int64
	x.read(&n)
	return n
}
func (x *Xx) beginReq(command Command) {
	x.write(command, x.sess)
}
func (x *Xx) beginResp() {
	x.readByte()
	// TODO: succ != 0 => read error
	x.readInt32()
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

func (x *Xx) size() int64 {
	x.beginReq(DB_SIZE)
	x.beginResp()
	return x.readInt64()
}

func (x *Xx) recordCount() int64 {
	x.beginReq(DB_COUNTRECORDS)
	x.beginResp()
	return x.readInt64()
}
func (x *Xx) loadRecord(cluster int16, position int64) []interface{} {
	x.beginReq(RECORD_LOAD)
	x.write(cluster, position)

	// TODO: Fetch plans
	// See https://github.com/nuvolabase/orientdb/wiki/Fetching-Strategies
	x.write("")

	// Ignore cache, don't load tombstones (?)
	x.write(byte(1), byte(0))


	x.beginResp()
	// Response: [(payload-status:byte)[(rec-content:bytes)(rec-ver:int)(rec-type:byte)]*]+

	// Record data:
	// Null record: (-2:short)
	// RID: (-3:short)(cluster:short)(position:long)
	// Record: (0:short)(rectype:byte)(clus:short)(pos:long)(ver:int)(content:bytes)

	out := make([]interface{}, 0, 4)
	for {
		stat := x.readByte()
		if stat == 0 {
			// No more records
			break
		}
		if stat == 2 {
			// Next record is a cache pre-fetch, to be loaded
			// into local cache; not part of the current request.
			// This isn't used as of 1.4.0 snapshot 15 (supposedly)
			panic("Cache pre-fetching is not yet supported")
		}
		rcontent := x.readBytes()
		x.readInt32() // TODO: record type with version field
		rtype := x.readByte()
		switch rtype {
		case 'b':
			out = append(out, rcontent)
		case 'f':
			out = append(out, string(rcontent))
		case 'd':
			doc := parse(string(rcontent))
			out = append(out, doc)
		default:
			panic(fmt.Sprintf("Unrecognized record type: %v",rtype))
		}
	}
	return out
}
