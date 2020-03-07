package util

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tellor-io/TellorMiner/config"
)

type MysqlConnection struct {
	DbHandle         *sql.DB
	log              *Logger
	TableName        string
	CurrentChallenge string
	UpdateTx         chan string
	StoreFoundBlock  chan FoundBlockInfo
}

type FoundBlockInfo struct {
	Challenge      string
	Difficulty     uint64
	RequestID      uint64
	PublicAddress  string
	Height         uint64
	Reward         uint64
	Nonce          string
	Jobid          uint64
	UserId         int32
	WorkerId       int64
	WorkerFullName string
}

func CreateMysqlConn(cfg *config.Config) *MysqlConnection {

	log := NewLogger("util", "mysql")
	tableName := cfg.MysqlConnectionInfo.Table
	path := strings.Join([]string{cfg.MysqlConnectionInfo.Username, ":",
		cfg.MysqlConnectionInfo.Password, "@tcp(",
		cfg.MysqlConnectionInfo.Host, ":",
		cfg.MysqlConnectionInfo.Port, ")/",
		cfg.MysqlConnectionInfo.Dbname, "?charset=utf8"}, "")

	log.Info("dbpath : " + path)

	dbHandle, _ := sql.Open("mysql", path)

	dbHandle.SetConnMaxLifetime(100)

	dbHandle.SetMaxIdleConns(10)

	if err := dbHandle.Ping(); err != nil {
		log.Error("opon database fail : %s", err)
		return nil
	}

	log.Info("connnect success")
	return &MysqlConnection{
		DbHandle:         dbHandle,
		log:              log,
		TableName:        tableName,
		CurrentChallenge: "",
		UpdateTx:         make(chan string, 1),
		StoreFoundBlock:  make(chan FoundBlockInfo, 1),
	}

}

func (handle *MysqlConnection) InsertFoundBlock(blockinfo FoundBlockInfo) bool {

	tx, err := handle.DbHandle.Begin()
	if err != nil {
		handle.log.Info("tx fail")
		return false
	}
	sql := "INSERT INTO " + handle.TableName
	sql += " (`challenge`,`difficulty`, `request_id`,`public_address`,`height`,`nonce`,`job_id`, `rewards`, `puid`, `worker_id`, `worker_full_name`,`created_at`) "
	sql += " values(?,?,?,?,?,?,?,?,?,?,?,?)"

	res, err := tx.Exec(sql,
		blockinfo.Challenge,
		blockinfo.Difficulty,
		blockinfo.RequestID,
		blockinfo.PublicAddress,
		blockinfo.Height,
		blockinfo.Nonce,
		blockinfo.Jobid,
		blockinfo.Reward,
		blockinfo.UserId,
		blockinfo.WorkerId,
		blockinfo.WorkerFullName,
		time.Now().Format("2006-01-02 15:04:05"),
	)
	if err != nil {
		handle.log.Error("Exec fail : ", err)
		tx.Commit()
		return false
	}
	lastid, _ := res.LastInsertId()
	handle.log.Info("query last id : %d", lastid)
	tx.Commit()
	handle.CurrentChallenge = blockinfo.Challenge
	return true
}

func (handle *MysqlConnection) UpdateFoundBlock(challenge string, hash string) bool {
	tx, err := handle.DbHandle.Begin()
	if err != nil {
		handle.log.Info("tx fail")
		return false
	}

	sql := fmt.Sprintf(
		"UPDATE %s SET hash='%s' WHERE challenge ='%s'",
		handle.TableName, hash, challenge,
	)

	res, err := tx.Exec(sql)
	if err != nil {
		handle.log.Info("exec failed:", err, ", sql:", sql)
		tx.Commit()
		return false
	}
	lastid, _ := res.LastInsertId()
	handle.log.Info("query last id : %d", lastid)
	tx.Commit()
	return true
}
