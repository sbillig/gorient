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
	fmt.Println("load:",x.loadRecord(91, 0))

}
