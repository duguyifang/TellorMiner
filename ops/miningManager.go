package ops

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	tellorCommon "github.com/tellor-io/TellorMiner/common"
	"github.com/tellor-io/TellorMiner/config"
	"github.com/tellor-io/TellorMiner/db"
	"github.com/tellor-io/TellorMiner/pow"
	"github.com/tellor-io/TellorMiner/util"
)

type WorkSource interface {
	GetWork(input chan *pow.Work) *pow.Work
}

type SolutionSink interface {
	Submit(context.Context, *pow.Result)
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

	// group, err := pow.SetupMiningGroup(cfg)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to setup miners: %s", err.Error())
	// }

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

		//if you make these buffered, think about the effects on synchronization!
		input := make(chan *pow.Work)
		output := make(chan *pow.Result)
		if cfg.RequestData > 0 {
			fmt.Println("Starting Data Requester")
			mgr.dataRequester.Start(ctx)
		}

		go mgr.ConsumeSolvedShare(output)
		go mgr.ConsumeMysqlMessage()

		sendWork := func() {
			work := mgr.tasker.GetWork(input)
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
		//send the initial challenge
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
