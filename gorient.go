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
	proto int16
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
func (x *Xx) readInt16() int16 {
	var n int16
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
func (x *Xx) readRid() Rid {
	return Rid{x.readInt16(), x.readInt64()}
}
func (x *Xx) beginReq(command Command) {
	x.write(command, x.sess)
}
func (x *Xx) beginResp() {
	err := x.readByte()

	// TODO: sess != x.sess
	x.readInt32()

	if err == 1 {
		// TODO: proper error handling
		fmt.Println("ERROR")
		x.readErrors()
		panic("Transaction failed")
	}

}

func (x *Xx) readErrors() {
	for x.readByte() == 1 {
		fmt.Println(x.readString(),	x.readString())
	}
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

	// Server sends protocol on connect
	x.proto = x.readInt16()

	if x.proto < 13 || x.proto > 15 {
		fmt.Println("Unrecognized protocol:",x.proto,
			"Continuing anyway.")
	}

	fmt.Println("db protocol:",x.proto)

	x.beginReq(DB_OPEN)
	x.write("gorient", "alpha", x.proto, "a client id")
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
	fmt.Println("cconfig:",x.readBytes())
	if x.proto >= 14 {
		fmt.Println("v:",x.readString())
	}
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
func (x *Xx) loadRecord(rid Rid, plan string) (Record, map[Rid]Record)  {
	x.beginReq(RECORD_LOAD)
	x.write(rid)

	// TODO: Fetch plans
	// See https://github.com/nuvolabase/orientdb/wiki/Fetching-Strategies
	x.write(plan)

	// Ignore cache, don't load tombstones (?)
	x.write(byte(1), byte(0))

	x.beginResp()
	// Response: [(payload-status:byte)[(rec-content:bytes)(rec-ver:int)(rec-type:byte)]*]+

	var pres map[Rid]Record
	var rec Record

	for {
		switch stat := x.readByte(); stat {
		case 1:
			content := x.readBytes()
			ver := x.readInt32()
			rtype := x.readByte()
			rec = Record{ver, recValue(rtype, content)}

		case 2:
			// Next record is a cache pre-fetch, to be loaded
			// into local cache; not part of the current request.
			if pres == nil {
				pres = make(map[Rid]Record, 1)
			}
			id, r := x.readRecord()
			pres[id] = r

		case 0:
			return rec, pres

		default:
			panic(fmt.Sprintf("Unrecognized payload status: %s\n", string(stat)))
		}
	}
	panic("")
}

func (x *Xx) readRecord() (Rid, Record) {
	// Null:(-2:short)
	// RID: (-3:short)(cluster:short)(position:long)
	// Rec:  (0:short)(rectype:byte)(clus:short)(pos:long)(ver:int)(content:bytes)

	rtype := x.readInt16()
	switch rtype {
	case 0:
		rtype := x.readByte()
		rid := x.readRid()
		ver := x.readInt32()
		content := x.readBytes()
		return rid, Record{ver, recValue(rtype, content)}
	case -2:
		// TODO
		panic("null record")
	case -3:
		rid := x.readRid()
		fmt.Println("RID sub:",rid)
		// TODO
		panic("RID record")
	}
	// TODO
	panic(fmt.Sprintf("Unrecognized record type: %v\n", rtype))
}
func recValue(rtype byte, content []byte) interface{} {
	switch rtype {
	case 'd':     return parse(string(content))
	case 'b','f': return content
	}
	panic(fmt.Sprintf("Unrecognized record format: %v",rtype))
}


// Execute a command string (ie. a query or script)
//
// class:
//   "c" (short for "com.orientechnologies.orient.core.sql.OCommandSQL")
//     Required for write commands.  Works for reads, but (apparently)
//     ignores the limit field.
//     The nodejs driver (node-orientdb) always uses this, unless a limit
//     or fetch plan is specified (in which case is uses OSQLAsynchQuery).
//
//   "q" ("com.orientechnologies.orient.core.sql.query.OSQLSynchQuery")
//     Only allows read queries (sends error and kills connection otherwise).
//
//   "s" ("com.orientechnologies.orient.core.command.script.OCommandScript")
//     Don't use.  I'll probably move this one to a separate function,
//     because it requires a language parameter (eg. "Javascript").
//
// mode: 's' (synchronous)
//       'a' (asynchronous)
//
//  'a' streams back records one at a time
//  's' packages records with a leading record count
func (x *Xx) command(q, class string, mode byte, lim int, fp string) {

	// NOTE: The orientdb network protocol docs seem to be wrong here.
	//  Should be:
	//    Request: (mode:byte)(payload-length:int)(payload)
	//  where
	//    payload = (class-name:string)(text:string)(limit:int)
	//              (fetchplan:string)(serialized-params:bytes)
	//  set limit = -1 for no limit
	//  set params = 0:int for no params
	//
	//  Note that the docs suggest that fetchplan should be left out for
	//  non-select queries. Sending an empty string (i.e. just the 0:int
	//  string length prefix) works fine.  Haven't tried leaving it out
	//  completely.

	x.beginReq(COMMAND)

	plen := 4 + len(class)
	plen += 4 + len(q)
	plen += 4 // limit
	plen += 4 + len(fp)
	plen += 4 // params (0:int for now)

	x.write(mode, int32(plen), class, q, int32(2), fp, int32(0))

	x.beginResp()

	if mode == 's' {
		stat := x.readByte()
		switch stat {
		case 'l':
			// (c:int)[(record)]{c}

			c := x.readInt32()
			for c > 0 {
				id, r := x.readRecord()
				fmt.Println("lrecord",c,":",id,r)
				c--
			}
		case 'r':
			id, r := x.readRecord()
			fmt.Println("rrecord:",id,r)
		case 'a':
			// (value:string/bytes)
			fmt.Println(x.readString())
		case 'n':
			fmt.Println("null record")
		}

		return
	}

	for {
		stat := x.readByte()
		switch stat {
		case 1:
			id, r := x.readRecord()
			fmt.Println("1record:",id,r)
		case 2:
			id, r := x.readRecord()
			fmt.Println("2record:",id,r)
		case 0:
			return
		default:
			return
		}
	}

}
