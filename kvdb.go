package mysql

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
)

type KvDb struct {
	db *sql.DB
}

type BaseConfig struct {
	User     string
	Password string
	DbName   string
}

func NewKvDb(config BaseConfig) (*KvDb, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@/%s", config.User, config.Password, config.DbName))
	if err != nil {
		return nil, err
	}
	return &KvDb{
		db: db,
	}, nil
}

func (db *KvDb) MustSet(table, k, v string) {
	panicIfError(db.Set(table, k, v))
}

func (db *KvDb) MustGet(table, k string) string {
	str, err := db.Get(table, k)
	panicIfError(err)
	return str
}

func (db *KvDb) MustDelete(table, k string) {
	_, err := db.Delete(table, k)
	panicIfError(err)
}

func (db *KvDb) MustGetRange(req GetRangeReq) []KV {
	result, err := db.GetRange(req)
	panicIfError(err)
	return result
}

func (db *KvDb) MustInsert(table, k, v string) {
	panicIfError(db.Insert(table, k, v))
}

var gBadSqlTableChar = []string{
	" ", ";",
}

func (db *KvDb) checkTable(table string) (bool, error) {
	if table == "" {
		return true, errors.New("c2dio84ic7 need table")
	}
	for _, item := range gBadSqlTableChar {
		if strings.Contains(table, " ") {
			return true, errors.New("r04m1kwoja syntax error, contains [" + item + "]")
		}
	}
	return false, nil
}
func (db *KvDb) Set(table, k, v string) error {
	if ok, err := db.checkTable(table); ok {
		return err
	}
	result, err := db.db.Exec(`REPLACE INTO `+table+` (k,v) VALUES(?,?)`, k, v)
	if err != nil {
		if strings.Contains(err.Error(), `Error 1146 (42S02): Table '`) &&
			strings.Contains(err.Error(), `' doesn't exist`) {
			_, err := db.db.Exec("CREATE TABLE IF NOT EXISTS `" + table + "`(`k` VARCHAR(255) NOT NULL,`v` text ,PRIMARY KEY ( `k` ))ENGINE=InnoDB DEFAULT CHARSET=utf8;")
			if err == nil {
				return db.Set(table, k, v)
			} else {
				fmt.Println("89e9ewq4y7", err)
			}
		}
		return newDbErr("rzzln8oxfc", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return newDbErr("u3xgcga54n", err)
	}
	if affected <= 0 {
		return errors.New("oqq9vdxo5b affected=" + strconv.Itoa(int(affected)))
	}
	return nil
}

func (db *KvDb) Get(table, k string) (string, error) {
	if ok, err := db.checkTable(table); ok {
		return "", err
	}
	res, err := db.db.Query(`SELECT v FROM `+table+` WHERE k = ?`, k)
	if err != nil {
		return "", newDbErr("j0prtj3ipe", err)
	}
	if res.Next() == false { //不存在不报错
		return "", nil
	}
	var v string
	err = res.Scan(&v)
	if err != nil {
		return "", newDbErr("wsp0s6wwb9", err)
	}
	return v, nil
}

func (db *KvDb) Delete(table, k string) (bool, error) {
	if ok, err := db.checkTable(table); ok {
		return false, err
	}
	result, err := db.db.Exec(`delete from `+table+` where k = ?`, k)
	if err != nil {
		return false, newDbErr("1zor614tgz", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, newDbErr("t0a58n132z", err)
	}
	if affected <= 0 {
		return false, errors.New("plzh7s02h7 affected=" + strconv.Itoa(int(affected)))
	}
	return true, nil
}

func (db *KvDb) Insert(table, k, v string) error {
	if ok, err := db.checkTable(table); ok {
		return err
	}
	result, err := db.db.Exec(`INSERT INTO `+table+` (k,v) VALUES(?,?)`, k, v)
	if err != nil {
		return newDbErr("483t1hrrb8", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return newDbErr("b3ffpxaqdu", err)
	}
	if affected <= 0 {
		return errors.New("v6ltvpojw2 affected=" + strconv.Itoa(int(affected)))
	}
	return nil
}

type GetRangeReq struct {
	Table      string
	Min        string
	Max        string
	MinInclude bool
	MaxInclude bool
	Limit      uint
	IsDesc     bool
}

func (req GetRangeReq) sql() (string, []interface{}) {
	var args []interface{}
	sql := bytes.NewBuffer(nil)
	sql.WriteString("select k,v from " + req.Table)
	haveMin := req.Min != ""
	if haveMin {
		sql.WriteString(" where k >")
		if req.MinInclude {
			sql.WriteString("=")
		}
		sql.WriteString(" ?")
		args = append(args, req.Min)
	}
	if req.Max != "" {
		if haveMin {
			sql.WriteString(" and")
		} else {
			sql.WriteString(" where")
		}
		sql.WriteString(" k<")
		if req.MaxInclude {
			sql.WriteString("=")
		}
		sql.WriteString(" ?")
		args = append(args, req.Max)
	}
	sql.WriteString(" order by k")
	if req.IsDesc {
		sql.WriteString(" desc")
	}
	if req.Limit != 0 {
		sql.WriteString(" limit ?")
		args = append(args, req.Limit)
	}
	return sql.String(), args
}

type KV struct {
	K string
	V string
}

func (db *KvDb) GetRange(req GetRangeReq) ([]KV, error) {
	if ok, err := db.checkTable(req.Table); ok {
		return nil, err
	}
	sql, args := req.sql()
	rows, err := db.db.Query(sql, args...)
	if err != nil {
		return nil, newDbErr("ruzlw67gu4", err)
	}
	var result []KV
	for rows.Next() {
		var k, v string
		err := rows.Scan(&k, &v)
		if err != nil {
			return nil, newDbErr("wbno72goua", err)
		}
		result = append(result, KV{
			K: k,
			V: v,
		})
	}
	return result, nil
}

func panicIfError(err error) {
	if err == nil {
		return
	}
	panic(err)
}
