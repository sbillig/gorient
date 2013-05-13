package gorient

import (
	"fmt"
	"testing"
)

func TestX(t *testing.T) {
	var x Xx
	err := x.open("localhost:2424", "demo", "admin", "admin")
	if err != nil {
		fmt.Println("err:",err)
		return
	}
	defer x.close()

	fmt.Println(x)
	fmt.Println("size:", x.size())
	fmt.Println("records:", x.recordCount())

//	x.command("create class testx", "c", 's', -1, "")
//	x.command("drop class testx", "c", 's', -1, "")
//	x.command("select * from ouser", "q", 's', -1, "")
//	x.command("select * from ouser", "q", 'a', -1, "")


//	rs := x.loadRecord(Rid{13, 0}, "*:-1")
//	fmt.Println("recs:",rs.Records)
//	fmt.Println("pres:",rs.Prefetch)

//	fmt.Println("load one:",x.loadRecord(Rid{13,0}, ""))

//  Returns a 'null' payload status:  (AsynchQuery with mode = 's')
//	x.command("select * from profile where nick = 'Neo'",
//		"com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery", 's', -1, "")

	x.command("select * from profile where nick = 'Neo'",
		"com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery", 'a', -1, "*:-1")

	x.command("select * from profile where nick = 'Neo'",
		"q", 's', -1, "*:-1")

//	x.command("select * from profile where nick = 'Neo'",
//		"q", 's', -1, "")
}
