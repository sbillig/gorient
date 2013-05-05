package gorient

import (
	"fmt"
	"reflect"
	"testing"
)

var testrec string = "ORole@name:\"reader\",inheritedRole:,embedded:(Blah@name:\"Bob\",age:32),rules:{\"byte\":12b,\"short\":245s,\"long\":58585l,\"float\":4.4f,\"double\":4.484844d,\"big\":0.58595884848484c,\"time\":1296279468000t,\"binary\":_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGx_,\"bool\":true,\"null\":null,\"date\":1306281600000a,\"database.command\":2,\"database.hook.record\":2}"


func BenchmarkLex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_,items := lex(testrec)
		for _ = range items {
		}
	}
}
func BenchmarkParse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parse(testrec)
	}
}

func TestTrailingSpace(t *testing.T) {
	s := `Person@name:"Raf",city:"TORINO",gender:"f"     `
	d := &Document{Class: "Person", Fields: map[string]interface{} {
			"name": "Raf",
			"city": "TORINO",
			"gender": "f",
		},
	}
	if !reflect.DeepEqual(parse(s), d) {
		t.Fail()
	}
}
func TestParse(t *testing.T) {
	s := `Profile@nick:"B \"POTUS\" Obama",follows:[],followers:[#10:5,#10:6],name:"Barack",age:51,location:#3:2,salary:120.3f,dog:(Animal@name:"Fido"),cat:(name:"Pip",age:7s),x:<1,2>`
//	s := "name:\"ORole\",id:0,defaultClusterId:3,clusterIds:[3],properties:[(name:\"mode\",type:17,offset:0,mandatory:false,notNull:false,min:,max:,linkedClass:,linkedType:,index:),(name:\"rules\",type:12,offset:1,mandatory:false,notNull:false,min:,max:,linkedClass:,linkedType:17,index:)]"
//

	d := parse(s)
	d1 := &Document{
		Class: "Profile",
		Fields: map[string]interface{} {
			"nick": "B \"POTUS\" Obama",
			"follows": []interface{} {},
			"followers": []interface{} {"#10:5", "#10:6"},
			"name": "Barack",
			"age": int32(51),
			"location": "#3:2",
			"salary": float32(120.3),
			"dog": &Document{
				"Animal",
				map[string]interface{} {"name":"Fido"},
			},
			"cat": &Document{
				Fields: map[string]interface{} {"name":"Pip","age":int16(7)},
			},
			"x": []interface{} {int32(1), int32(2)},
		},
	}

	if !reflect.DeepEqual(d, d1) {
		t.Fail()
	}

	for k, v := range d.Fields {
		if !reflect.DeepEqual(v, d1.Fields[k]) {
			fmt.Println("failed on",k)
			t.Fail()
		}
	}
}
