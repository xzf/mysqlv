package mysqlv

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
)

type Db struct {
	db     *sql.DB
	dbName string
}

type BaseConfig struct {
	Addr     string
	User     string
	Password string
	DbName   string
}

func NewKvDb(config BaseConfig) (*Db, error) {
	if config.DbName == "" || config.User == "" {
		return nil, errors.New(`dxcf0g8gnk config.DbName == "" || config.User == ""`)
	}
	if ok, err := checkTable(config.DbName); ok {
		return nil, err
	}
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@%s/%s", config.User, config.Password, config.Addr, config.DbName))
	if err != nil {
		return nil, err
	}
	result, err := &Db{
		db: db,
	}, nil
	row, err := db.Query("show databases like '" + config.DbName + "';")
	if err == nil && row.Next() {
		return result, nil
	}
	tmp, err := sql.Open("mysql", fmt.Sprintf("%s:%s@/%s", config.User, config.Password, "mysql"))
	if err != nil {
		return nil, err
	}
	_, err = tmp.Exec(`CREATE DATABASE ` + config.DbName)
	if err != nil {
		return nil, err
	}
	row, err = db.Query("show databases like '" + config.DbName + "';")
	if err == nil && row.Next() {
		return result, nil
	}
	return nil, err
}

func (db *Db) tryCreateDataBase(dbName string) error {
	if ok, err := checkTable(dbName); ok {
		return err
	}
	_, err := db.db.Exec("CREATE DATABASE " + dbName)
	return err
}

func (db *Db) MustSet(table, k, v string) {
	panicIfError(db.Set(table, k, v))
}

func (db *Db) MustGet(table, k string) string {
	str, err := db.Get(table, k)
	panicIfError(err)
	return str
}

func (db *Db) MustDelete(table, k string) {
	_, err := db.Delete(table, k)
	panicIfError(err)
}

func (db *Db) MustGetRange(req GetRangeReq) []KV {
	result, err := db.GetRange(req)
	panicIfError(err)
	return result
}

func (db *Db) MustInsert(table, k, v string) {
	panicIfError(db.Insert(table, k, v))
}

var gBadSqlTableChar = []string{
	" ", ";",
}

func checkTable(table string) (bool, error) {
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

func (db *Db) isTableNotExistError(err error) bool {
	errMsg := err.Error()
	return strings.Contains(errMsg, `Error 1146 (42S02): Table '`) &&
		strings.Contains(errMsg, `' doesn't exist`)
}

func (db *Db) tryCreateTable(table string) error {
	_, err := db.db.Exec("CREATE TABLE IF NOT EXISTS `" + table + "`(`k` VARCHAR(255) NOT NULL,`v` MEDIUMTEXT ,PRIMARY KEY ( `k` ))ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	return err
}

func (db *Db) Set(table, k, v string) error {
	if ok, err := checkTable(table); ok {
		return err
	}
	result, err := db.db.Exec(`REPLACE INTO `+table+` (k,v) VALUES(?,?)`, k, v)
	if err != nil {
		if db.isTableNotExistError(err) {
			err = db.tryCreateTable(table)
			if err != nil {
				return err
			}
			return db.Set(table, k, v)
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

func (db *Db) Get(table, k string) (string, error) {
	if ok, err := checkTable(table); ok {
		return "", err
	}
	res, err := db.db.Query(`SELECT v FROM `+table+` WHERE k = ?`, k)
	if err != nil {
		if db.isTableNotExistError(err) {
			err = db.tryCreateTable(table)
			if err != nil {
				return "", err
			}
			return db.Get(table, k)
		}
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

func (db *Db) Delete(table, k string) (bool, error) {
	if ok, err := checkTable(table); ok {
		return false, err
	}
	result, err := db.db.Exec(`delete from `+table+` where k = ?`, k)
	if err != nil {
		if db.isTableNotExistError(err) {
			err = db.tryCreateTable(table)
			if err != nil {
				return false, err
			}
			return db.Delete(table, k)
		}
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

func (db *Db) Insert(table, k, v string) error {
	if ok, err := checkTable(table); ok {
		return err
	}
	result, err := db.db.Exec(`INSERT INTO `+table+` (k,v) VALUES(?,?)`, k, v)
	if err != nil {
		if db.isTableNotExistError(err) {
			err = db.tryCreateTable(table)
			if err != nil {
				return err
			}
			return db.Insert(table, k, v)
		}
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

func (db *Db) GetRange(req GetRangeReq) ([]KV, error) {
	if ok, err := checkTable(req.Table); ok {
		return nil, err
	}
	sql, args := req.sql()
	rows, err := db.db.Query(sql, args...)
	if err != nil {
		if db.isTableNotExistError(err) {
			err = db.tryCreateTable(req.Table)
			if err != nil {
				return nil, err
			}
			return db.GetRange(req)
		}
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
