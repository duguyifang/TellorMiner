package ops

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/tellor-io/TellorMiner/pow"
	"github.com/tellor-io/TellorMiner/util"
)

type WorkMessage struct {
	Challenge     string   `json:"challenge"`
	Difficulty    *big.Int `json:"difficulty"`
	RequestID     uint64   `json:"request_id"`
	PublicAddress string   `json:"public_address"`
	Height        uint64   `json:"height"`
	Ntime         uint64   `json:"ntime"`
}

type ShareMessage struct {
	RequestID      uint64 `json:"request_id"`
	Nonce          string `json:"nonce"`
	Jobid          uint64 `json:"job_id"`
	UserId         int32  `json:"userId"`
	WorkerId       int64  `json:"workerId"`
	WorkerFullName string `json:"workerFullName"`
	Height         uint64 `json:"height"`
	Ntime          uint64 `json:"ntime"`
	PublicAddress  string `json:"publicAddress"`
}

type MiningJob struct {
	Reward uint64
	Work   *pow.Work
}

type RpcMessage struct {
	Jsonrpc string `json:"jsonrpc"`
	Id      int    `json:"id"`
	Result  string `json:"result"`
}

func decodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return b
}

func (mgr *MiningMgr) SendJobToKafka(work *pow.Work, height uint64) {

	command := WorkMessage{
		fmt.Sprintf("%x", work.Challenge.Challenge),
		work.Challenge.Difficulty,
		work.Challenge.RequestID.Uint64(),
		work.PublicAddr,
		height,
		height}
	bytes, _ := json.Marshal(command)
	mgr.log.Info("====> send work to kafka : %s", string(bytes))
	mgr.controllerProducer.WriteMessages(context.Background(), kafka.Message{Value: []byte(bytes)})
}

func (mgr *MiningMgr) ConsumeSolvedShare(output chan *pow.Result) {
	mgr.processorConsumer.SetOffset(kafka.LastOffset)
	for {
		m, err := mgr.processorConsumer.ReadMessage(context.Background())
		if err != nil {
			mgr.log.Info("read kafka failed: ", err)
			continue
		}

		response := new(ShareMessage)
		err = json.Unmarshal(m.Value, response)
		if err != nil {
			mgr.log.Info("Parse Result Failed: ", err)
			continue
		}

		mgr.log.Info(">>>>>>>> received solved share ", response)
		mgr.mutex.Lock()
		job, ok := mgr.workmap[response.Ntime]
		mgr.mutex.Unlock()
		if ok {
			mgr.log.Info("found job in work map, to submit... ")
			mgr.log.Info("challenge : %s", fmt.Sprintf("%x", job.Work.Challenge.Challenge))
			mgr.log.Info("current miner publicaddress : %s", job.Work.PublicAddr)

			if strings.Compare(job.Work.PublicAddr, response.PublicAddress) == 0 {
				nonce := string(decodeHex(response.Nonce))

				output <- &pow.Result{Work: job.Work, Nonce: nonce}
			} else {
				mgr.log.Info("share publicaddress : %s", job.Work.PublicAddr)
				continue
			}

		} else {
			mgr.log.Error("cannot find the job in height : %d", response.Height)
			continue
		}

		var foundblockinfo util.FoundBlockInfo
		foundblockinfo.Challenge = fmt.Sprintf("%x", job.Work.Challenge.Challenge)
		foundblockinfo.Difficulty = job.Work.Challenge.Difficulty.Uint64()
		foundblockinfo.RequestID = job.Work.Challenge.RequestID.Uint64()
		foundblockinfo.PublicAddress = job.Work.PublicAddr
		foundblockinfo.Height = response.Height
		foundblockinfo.Nonce = response.Nonce
		foundblockinfo.Jobid = response.Jobid
		foundblockinfo.UserId = response.UserId
		foundblockinfo.WorkerId = response.WorkerId
		foundblockinfo.WorkerFullName = response.WorkerFullName
		foundblockinfo.Reward = job.Reward

		mgr.mysqlHandle.StoreFoundBlock <- foundblockinfo
	}
}

func (mgr *MiningMgr) ConsumeMysqlMessage() {
	for {
		select {
		case tx := <-mgr.mysqlHandle.UpdateTx:
			mgr.mysqlHandle.UpdateFoundBlock(mgr.mysqlHandle.CurrentChallenge, tx)

		case foundblockinfo := <-mgr.mysqlHandle.StoreFoundBlock:
			mgr.mysqlHandle.CurrentChallenge = foundblockinfo.Challenge
			if ok := mgr.mysqlHandle.InsertFoundBlock(foundblockinfo); !ok {
				mgr.log.Error("inset foundblock to mysql failed ")
			} else {
				mgr.log.Info("insert foundblock to mysql success!")
			}
		}
	}
}

func (mgr *MiningMgr) GetCurrentRewards() uint64 {

	data := "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[{\"to\":\"0x0ba45a8b5d5575935b8158a88c631e9f9c95a2e5\",\"data\":\"0x612c8f7f9b6853911475b07474368644a0d922ee13bc76a15cd3e97d3e334326424a47d4\"}, \"latest\"]}"

	req, err := http.NewRequest("POST", mgr.ethurl, bytes.NewBuffer([]byte(data)))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	cli := &http.Client{}
	resp, err := cli.Do(req)
	if err != nil {
		mgr.log.Error("failed to get eth height from node: %s", err.Error())
		return 0
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		mgr.log.Error("failed to read response: %s", err.Error())
		return 0
	}

	var j = new(RpcMessage)
	err = json.Unmarshal(body, &j)
	if err != nil {
		mgr.log.Error("Error decoding job json: %s", err.Error())
		return 0
	}
	mgr.log.Info("read response: %s", string(body))

	reward, _ := strconv.ParseUint(j.Result, 0, 64)
	return reward
}
