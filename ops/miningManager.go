package ops

import (
	"context"
	"fmt"
	"os"
	"time"

	tellorCommon "github.com/tellor-io/TellorMiner/common"
	"github.com/tellor-io/TellorMiner/config"
	"github.com/tellor-io/TellorMiner/db"
	"github.com/tellor-io/TellorMiner/pow"
	"github.com/tellor-io/TellorMiner/util"

	"bytes"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
)

type WorkSource interface {
	GetWork() *pow.Work
}

type SolutionSink interface {
	Submit(context.Context, *pow.Result)
}

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

//MiningMgr holds items for mining and requesting data
type MiningMgr struct {
	exitCh             chan os.Signal
	log                *util.Logger
	Running            bool
	group              *pow.MiningGroup
	tasker             WorkSource
	solHandler         SolutionSink
	dataRequester      *DataRequester
	requesterExit      chan os.Signal
	mutex              sync.Mutex
	ethurl             string
	controllerProducer *kafka.Writer
	processorConsumer  *kafka.Reader
	mysqlHandle        *util.MysqlConnection
	workmap            map[uint64]MiningJob
}

//CreateMiningManager creates a new manager that mananges mining and data requests
func CreateMiningManager(ctx context.Context, exitCh chan os.Signal, submitter tellorCommon.TransactionSubmitter) (*MiningMgr, error) {
	cfg := config.GetConfig()

	mng := &MiningMgr{
		exitCh:     exitCh,
		log:        util.NewLogger("ops", "MiningMgr"),
		Running:    false,
		tasker:     nil,
		solHandler: nil,
		ethurl:     cfg.NodeURL,
	}

	proxy := ctx.Value(tellorCommon.DataProxyKey).(db.DataServerProxy)
	mng.tasker = pow.CreateTasker(cfg, proxy)
	mng.solHandler = pow.CreateSolutionHandler(cfg, submitter, proxy)

	mng.processorConsumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{cfg.Kafka.Brokers},
		Topic:     cfg.Kafka.SolvedShareTopic,
		Partition: 0,
		MinBytes:  128,  // 128B
		MaxBytes:  10e6, // 10MB
	})

	mng.controllerProducer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:          []string{cfg.Kafka.Brokers},
		Topic:            cfg.Kafka.JobTopic,
		Balancer:         &kafka.LeastBytes{},
		CompressionCodec: snappy.NewCompressionCodec(),
	})

	mng.mysqlHandle = util.CreateMysqlConn(cfg)

	mng.workmap = make(map[uint64]MiningJob)
	return mng, nil
}

//Start will start the mining run loop
func (mgr *MiningMgr) Start(ctx context.Context) {
	mgr.Running = true
	go func(ctx context.Context) {
		cfg := config.GetConfig()

		ticker := time.NewTicker(cfg.MiningInterruptCheckInterval.Duration)
		input := make(chan *pow.Work)
		output := make(chan *pow.Result)

		go mgr.ConsumeSolvedShare(output)
		go mgr.ConsumeMysqlMessage()

		sendWork := func() {
			work := mgr.tasker.GetWork()
			if work != nil {
				height := time.Now().Unix()
				mgr.SendJobToKafka(work, uint64(height))

				mgr.mutex.Lock()
				for k, _ := range mgr.workmap {
					mgr.log.Info(" delete old job request id : %d", k)
					delete(mgr.workmap, k)
				}
				reward := mgr.GetCurrentRewards()
				miningjob := MiningJob{
					reward,
					work,
				}
				mgr.workmap[uint64(height)] = miningjob
				mgr.mutex.Unlock()
			} else {
				mgr.log.Info("====> current work is nill discard it")
			}
		}

		sendWork()
		updateChan := util.GetInstance()
		updateChan.IsReady = true

		for {
			select {
			case <-mgr.exitCh:
				input <- nil

			case result := <-output:
				if result == nil {
					mgr.Running = false
					return
				}
				mgr.solHandler.Submit(ctx, result)
				sendWork()

			case _ = <-ticker.C:
				mgr.log.Info("it's time to get work")
				sendWork()
			case _ = <-updateChan.UpdateChallenge:
				mgr.log.Info("==> challenge  have updated so get work now")
				sendWork()
			case tx := <-updateChan.UpdateTx:
				mgr.mysqlHandle.UpdateTx <- tx
			}
		}
	}(ctx)
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

func decodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return b
}
