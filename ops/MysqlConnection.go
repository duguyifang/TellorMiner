package ops

import (
    "strings"
    "database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tellor-io/TellorMiner/util"
	"github.com/tellor-io/TellorMiner/config"
)
type MysqlConnection struct {
	DbHandle *sql.DB
	log     *util.Logger
	TableName string
}

type FoundBlockInfo struct {
	Challenge         string
	Difficulty        uint64
	RequestID         uint64
	PublicAddress     string
	Height            uint64
	Nonce             string
	Jobid             string
	UserId            int32
	WorkerId          int64
	WorkerFullName    string
}

func (handle *MysqlConnection) CreateMysqlConn(cfg *config.Config) {
	
	handle.log = util.NewLogger("ops", "mysql")
	handle.TableName = cfg.MysqlConnectionInfo.Table
	path := strings.Join([]string{cfg.MysqlConnectionInfo.Username, ":", 
	cfg.MysqlConnectionInfo.Password, "@tcp(",
	cfg.MysqlConnectionInfo.Host, ":", 
	cfg.MysqlConnectionInfo.Port, ")/", 
	cfg.MysqlConnectionInfo.Dbname, "?charset=utf8"}, "")

    handle.log.Info("dbpath : " + path )
	handle.DbHandle, _ = sql.Open("mysql", path)
    
    handle.DbHandle.SetConnMaxLifetime(100)
    
    handle.DbHandle.SetMaxIdleConns(10)
    
    if err := handle.DbHandle.Ping(); err != nil{
        handle.log.Error("opon database fail : %s", err)
        return
    }

    handle.log.Info("connnect success")

}


func (handle *MysqlConnection) InsertFoundBlock(blockinfo FoundBlockInfo) (bool){

    tx, err := handle.DbHandle.Begin()
    if err != nil{
        handle.log.Info("tx fail")
        return false
    }
    sql := "INSERT INTO " + handle.TableName 
    sql += " (`challenge`,`difficulty`, `request_id`,`public_address`,`height`,`nonce`,`job_id`, `user_id`, `work_id`, `work_full_name`) "
    sql += " values(?,?,?,?,?,?,?,?,?,?)"

	res, err := tx.Exec(sql,
		blockinfo.Challenge,
		blockinfo.Difficulty, 
		blockinfo.RequestID, 
		blockinfo.PublicAddress,
		blockinfo.Height,
		blockinfo.Nonce,
		blockinfo.Jobid,
		blockinfo.UserId,
		blockinfo.WorkerFullName,
	)
    if err != nil{
        handle.log.Error("Exec fail : ", err)
        tx.Commit()
        return false
    }
    lastid, _ := res.LastInsertId();
    handle.log.Info("query last id : %d",lastid)
    tx.Commit()
    return true
}