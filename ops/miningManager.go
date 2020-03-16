package ops

import (
	"context"
	"fmt"
	tellorCommon "github.com/tellor-io/TellorMiner/common"
	"github.com/tellor-io/TellorMiner/config"
	"github.com/tellor-io/TellorMiner/db"
	"github.com/tellor-io/TellorMiner/pow"
	"github.com/tellor-io/TellorMiner/util"
	"os"
	"time"

	"bytes"
	"strconv"
	"io/ioutil"
	"net/http"
	"math/big"
	"encoding/json"
	"encoding/hex"
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
	Challenge     string      `json:"challenge"`
	Difficulty    *big.Int    `json:"difficulty"`
	RequestID     uint64      `json:"request_id"`
	PublicAddress string      `json:"public_address"`
	Height        uint64      `json:"height"`
}

// {"job_id":6801736108045500417,"request_id":5,"timestamp":144,"nonce":0100000073237e298804f453,"userId":1,"workerId":8892583734397622546,"workerFullName":"user1.simulator-00000"}

type ShareMessage struct {
	RequestID         uint64      `json:"request_id"`
	Nonce             string      `json:"nonce"` 
	Jobid             uint64      `json:"job_id"`
	UserId            int32       `json:"userId"`
	WorkerId          int64       `json:"workerId"`
	WorkerFullName    string      `json:"workerFullName"`
	Height            uint64      `json:"height"`
}

type MiningJob struct {
	NTime             int64
	Work              *pow.Work
}

type HeightMessage struct {
	Jsonrpc         string    `json:"jsonrpc"`
	Id              int       `json:"id"`
	Result          string    `json:"result"`
}

//MiningMgr holds items for mining and requesting data
type MiningMgr struct {
	//primary exit channel
	exitCh  chan os.Signal
	log     *util.Logger
	Running bool
	group  *pow.MiningGroup
	tasker WorkSource
	solHandler SolutionSink
	dataRequester *DataRequester
	//data requester's exit channel
	requesterExit chan os.Signal

	ethurl string
	controllerProducer *kafka.Writer
	processorConsumer *kafka.Reader
	mysqlHandle       *util.MysqlConnection
	workmap map[uint64] MiningJob
}

//CreateMiningManager creates a new manager that mananges mining and data requests
func CreateMiningManager(ctx context.Context, exitCh chan os.Signal, submitter tellorCommon.TransactionSubmitter) (*MiningMgr, error) {
	cfg := config.GetConfig()

	mng := &MiningMgr{
		exitCh:  exitCh,
		log:     util.NewLogger("ops", "MiningMgr"),
		Running: false,
		// group:   group,
		tasker:  nil,
		solHandler: nil,
		ethurl:  cfg.NodeURL,
	}

	proxy := ctx.Value(tellorCommon.DataProxyKey).(db.DataServerProxy)
	mng.tasker = pow.CreateTasker(cfg, proxy)
	mng.solHandler = pow.CreateSolutionHandler(cfg, submitter, proxy)

	mng.processorConsumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{ cfg.Kafka.Brokers},
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

		sendWork := func () {
			work := mgr.tasker.GetWork()
			if work != nil {
				height := time.Now().Unix()
				mgr.SendJobToKafka(work, uint64(height))
				miningjob := MiningJob{
					time.Now().Unix(),
					work,
				}
				for k, _ := range mgr.workmap {
					mgr.log.Info("====> delete old job request id : %d", k)
					delete(mgr.workmap,k)
				}
				mgr.workmap[uint64(height)] = miningjob
			} else {
				mgr.log.Info("====> current work is nill ")
			}
		}
		sendWork()
		updateChan := util.GetInstance()
		updateChan.IsReady = true
		for {
			select {
			case <-mgr.exitCh:
				input <- nil

			case result := <- output:
				if result == nil {
					mgr.Running = false
					return
				}
				mgr.solHandler.Submit(ctx, result)
				sendWork()

			case _ = <-ticker.C:
				mgr.log.Info("====> it's time to get work")
				sendWork()
			case _ = <- updateChan.UpdateChallenge:
				mgr.log.Info("====> challenge  have updated so get work now")
				sendWork()
			case tx := <- updateChan.UpdateTx:
				mgr.log.Info("====> received update tx message ,translate %s ", tx)
				mgr.mysqlHandle.UpdateTx <- tx
			}
		}
	}(ctx)
}


func (mgr *MiningMgr)SendJobToKafka(work *pow.Work, height uint64) {

	command := WorkMessage{
	    fmt.Sprintf("%x", work.Challenge.Challenge),
	    work.Challenge.Difficulty,
	    work.Challenge.RequestID.Uint64(),
		work.PublicAddr,
	    height}
	bytes, _ := json.Marshal(command)
	mgr.log.Info("====> send work to kafka : %s", string(bytes))
	mgr.controllerProducer.WriteMessages(context.Background(), kafka.Message{Value: []byte(bytes)})
}

func (mgr *MiningMgr)ConsumeSolvedShare(output chan *pow.Result) {
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
		job, ok := mgr.workmap[response.Height]
		if ok {
			mgr.log.Info("found job in work map, to submit... ")
			mgr.log.Info("challenge : %s", fmt.Sprintf("%x", job.Work.Challenge.Challenge))
			mgr.log.Info("publicaddress : %s", job.Work.PublicAddr)
			mgr.log.Info("Nonce : %s", response.Nonce)
			nonce := string(decodeHex(response.Nonce))

			output <- &pow.Result{Work:job.Work, Nonce:nonce}

		} else {
			mgr.log.Error("cannot find the job in height : %d", response.Height)
			continue
		}

		var foundblockinfo util.FoundBlockInfo
		foundblockinfo.Challenge =  fmt.Sprintf("%x", job.Work.Challenge.Challenge)
		foundblockinfo.Difficulty = job.Work.Challenge.Difficulty.Uint64()
		foundblockinfo.RequestID = job.Work.Challenge.RequestID.Uint64()
		foundblockinfo.PublicAddress = job.Work.PublicAddr
		foundblockinfo.Height = response.Height
		foundblockinfo.Nonce = response.Nonce
		foundblockinfo.Jobid = response.Jobid
		foundblockinfo.UserId = response.UserId
		foundblockinfo.WorkerId = response.WorkerId
		foundblockinfo.WorkerFullName = response.WorkerFullName

		mgr.mysqlHandle.StoreFoundBlock <- foundblockinfo
	}
}

func (mgr *MiningMgr)GetCurrentEthHeight() uint64 {

	data := "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\"}"

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

	var j = new(HeightMessage)
	err = json.Unmarshal(body, &j)
	if err != nil {
		mgr.log.Error("Error decoding job json: %s", err.Error())
		return 0
	}
	mgr.log.Info("read response: %s", string(body))
	height, _ := strconv.ParseUint(j.Result, 0, 64)
	return height
}

func (mgr *MiningMgr)ConsumeMysqlMessage() {
    for{
		select {
		case tx := <- mgr.mysqlHandle.UpdateTx:
			mgr.log.Info("====> update tx to mysql tx: %s", tx)
			mgr.log.Info("====> update tx to mysql challenge: %s", mgr.mysqlHandle.CurrentChallenge)
			mgr.mysqlHandle.UpdateFoundBlock(mgr.mysqlHandle.CurrentChallenge, tx)

		case foundblockinfo := <- mgr.mysqlHandle.StoreFoundBlock:
			mgr.mysqlHandle.CurrentChallenge = foundblockinfo.Challenge
			mgr.log.Info("====> insert foundblock to mysql %s ", mgr.mysqlHandle.CurrentChallenge)
			if ok := mgr.mysqlHandle.InsertFoundBlock(foundblockinfo); !ok {
		        mgr.log.Error("inset foundblock to mysql failed ")
		    } else {
				mgr.log.Info("insert foundblock to mysql success!")
			}
		}
	}
}

func decodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return b
}