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
<<<<<<< HEAD
=======
	"os"
	"time"

	"log"
	"bytes"
	"strconv"
	"io/ioutil"
	"net/http"
	"math/big"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"


>>>>>>> get job and send to kafka
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
	RequestID     *big.Int    `json:"request_id"`
	PublicAddress string      `json:"public_address"`
	Height        uint64      `json:"height"`
}

type ShareMessage struct {
	RequestID         *big.Int    `json:"request_id"`
	Nonce             string      `json:"nonce"`
	Jobid             string      `json:"job_id"`
	UserId            int32       `json:"userId"`
	WorkerId          int64       `json:"workerId"`
	WorkerFullName    string      `json:"workerFullName"`
	Height            uint64      `json:"height"`
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
<<<<<<< HEAD

	group      *pow.MiningGroup
	tasker     WorkSource
=======
	group  *pow.MiningGroup
	tasker WorkSource
>>>>>>> get job and send to kafka
	solHandler SolutionSink
	dataRequester *DataRequester
	//data requester's exit channel
	requesterExit chan os.Signal

	ethurl string
	controllerProducer *kafka.Writer
	processorConsumer *kafka.Reader
	mysqlHandle       MysqlConnection
	workmap map[*big.Int]*pow.Work
}

//CreateMiningManager creates a new manager that mananges mining and data requests
func CreateMiningManager(ctx context.Context, exitCh chan os.Signal, submitter tellorCommon.TransactionSubmitter) (*MiningMgr, error) {
	cfg := config.GetConfig()

	// group, err := pow.SetupMiningGroup(cfg)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to setup miners: %s", err.Error())
	// }

	mng := &MiningMgr{
<<<<<<< HEAD
		exitCh:     exitCh,
		log:        util.NewLogger("ops", "MiningMgr"),
		Running:    false,
		group:      group,
		tasker:     nil,
=======
		exitCh:  exitCh,
		log:     util.NewLogger("ops", "MiningMgr"),
		Running: false,
		// group:   group,
		tasker:  nil,
>>>>>>> get job and send to kafka
		solHandler: nil,
		ethurl:  cfg.NodeURL,
	}

<<<<<<< HEAD
	if cfg.EnablePoolWorker {
		pool := pow.CreatePool(cfg, group)
		mng.tasker = pool
		mng.solHandler = pool
	} else {
		proxy := ctx.Value(tellorCommon.DataProxyKey).(db.DataServerProxy)
		mng.tasker = pow.CreateTasker(cfg, proxy)
		mng.solHandler = pow.CreateSolutionHandler(cfg, submitter, proxy)
		if cfg.RequestData > 0 {
			fmt.Println("dataRequester created")
			mng.dataRequester = CreateDataRequester(exitCh, submitter, 0, proxy)
		}
	}
=======
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

	mng.mysqlHandle.CreateMysqlConn(cfg)

	mng.workmap = make(map[*big.Int]*pow.Work)
>>>>>>> get job and send to kafka
	return mng, nil
}

//Start will start the mining run loop
func (mgr *MiningMgr) Start(ctx context.Context) {
	mgr.Running = true
	go func(ctx context.Context) {
		cfg := config.GetConfig()

		ticker := time.NewTicker(cfg.MiningInterruptCheckInterval.Duration)

		//if you make these buffered, think about the effects on synchronization!
		input := make(chan *pow.Work)
		output := make(chan *pow.Result)
		if cfg.RequestData > 0 {
			fmt.Println("Starting Data Requester")
			mgr.dataRequester.Start(ctx)
		}
		//start the mining group
		go mgr.ConsumeSolvedShare(output)

		// sends work to the mining group
		sendWork := func() {
			//if its nil, nothing new to report
			work := mgr.tasker.GetWork()
			log.Printf("====>  get work return <====")
			if work != nil {
				mgr.SendJobToKafka(work)
				mgr.workmap[work.Challenge.RequestID] = work
			} else {
				mgr.log.Info("====> current work is nill ")
			}
		}
		//send the initial challenge
		sendWork()
		for {
			select {
			//boss wants us to quit for the day
			case <-mgr.exitCh:
				//exit
				input <- nil

			//found a solution
			case result := <-output:
				if result == nil {
					mgr.Running = false
					return
				}
				mgr.solHandler.Submit(ctx, result)
				sendWork()

			//time to check for a new challenge
			case _ = <-ticker.C:
				log.Printf("====> it's time to get work <====")
				mgr.log.Info("====> it's time to get work")
				sendWork()
			}
		}
	}(ctx)
}


func (mgr *MiningMgr)SendJobToKafka(work *pow.Work) {
	height := mgr.GetCurrentEthHeight()
	command := WorkMessage{
	    fmt.Sprintf("%x", work.Challenge.Challenge),
	    work.Challenge.Difficulty,
	    work.Challenge.RequestID,
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

		work, ok := mgr.workmap[response.RequestID]
		if ok {
			mgr.log.Info("found wor in work map, to submit... ")
			output <- &pow.Result{Work:work, Nonce:response.Nonce}

		} else {
			mgr.log.Error("cannot find the job in workmap")
			continue
		}

		var foundblockinfo FoundBlockInfo
		foundblockinfo.Challenge =  fmt.Sprintf("%x", work.Challenge.Challenge)
		foundblockinfo.Difficulty = work.Challenge.Difficulty.Uint64()
		foundblockinfo.RequestID = work.Challenge.RequestID.Uint64()
		foundblockinfo.PublicAddress = work.PublicAddr
		foundblockinfo.Height = response.Height
		foundblockinfo.Nonce = response.Nonce
		foundblockinfo.Jobid = response.Jobid
		foundblockinfo.UserId = response.UserId
		foundblockinfo.WorkerId = response.WorkerId
		foundblockinfo.WorkerFullName = response.WorkerFullName

		if ok = mgr.mysqlHandle.InsertFoundBlock(foundblockinfo); !ok {
			mgr.log.Error("inset found block to mysql failed ")
		}
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
