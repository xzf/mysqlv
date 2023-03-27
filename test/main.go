package main

import (
	"fmt"
	"github.com/xzf/mysqlv"
)

func main() {
	db, err := mysqlv.NewKvDb(mysqlv.BaseConfig{
		User:     "root",
		Password: "",
		DbName:   "test111111",
	})
	if err != nil {
		fmt.Println("fqk2s8mkji", err)
		return
	}
	fmt.Println(db.Set("aaa", "1", "1"))
	fmt.Println(db.Get("aaa", "1"))
	fmt.Println(db.Delete("aaa", "1"))
	fmt.Println(db.Get("aaa", "1"))
	fmt.Println(db.Insert("aaa", "1", "2"))
	fmt.Println(db.Get("aaa", "1"))
	fmt.Println(db.GetRange(mysqlv.GetRangeReq{
		Table: "aaa",
	}))
}
