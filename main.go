package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	cli "github.com/jawher/mow.cli"
	tellorCommon "github.com/tellor-io/TellorMiner/common"
	"github.com/tellor-io/TellorMiner/config"
	"github.com/tellor-io/TellorMiner/contracts"
	"github.com/tellor-io/TellorMiner/contracts1"
	"github.com/tellor-io/TellorMiner/db"
	"github.com/tellor-io/TellorMiner/ops"
	"github.com/tellor-io/TellorMiner/rpc"
	"github.com/tellor-io/TellorMiner/util"
	"log"
	"math/big"
	"os"
	"os/signal"
	"time"
)

var ctx context.Context

func ErrorHandler(err error, operation string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s failed: %s\n", operation, err.Error())
		cli.Exit(-1)
	}
}

func buildContext() error {
	cfg := config.GetConfig()

	if !cfg.EnablePoolWorker {
		//create an rpc client
		client, err := rpc.NewClient(cfg.NodeURL)
		if err != nil {
			log.Fatal(err)
		}
		//create an instance of the tellor master contract for on-chain interactions
		contractAddress := common.HexToAddress(cfg.ContractAddress)
		masterInstance, err := contracts.NewTellorMaster(contractAddress, client)
		transactorInstance, err := contracts1.NewTellorTransactor(contractAddress, client)
		if err != nil {
			log.Fatal(err)
		}

		ctx = context.WithValue(context.Background(), tellorCommon.ClientContextKey, client)
		ctx = context.WithValue(ctx, tellorCommon.ContractAddress, contractAddress)
		ctx = context.WithValue(ctx, tellorCommon.MasterContractContextKey, masterInstance)
		ctx = context.WithValue(ctx, tellorCommon.TransactorContractContextKey, transactorInstance)

		privateKey, err := crypto.HexToECDSA(cfg.PrivateKey)
		if err != nil {
			return fmt.Errorf("problem getting private key: %s", err.Error())
		}
		ctx = context.WithValue(ctx, tellorCommon.PrivateKey, privateKey)

		publicKey := privateKey.Public()
		publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("error casting public key to ECDSA")
		}

		publicAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
		ctx = context.WithValue(ctx, tellorCommon.PublicAddress, publicAddress)

		//Issue #55, halt if client is still syncing with Ethereum network
		s, err := client.IsSyncing(ctx)
		if err != nil {
			return fmt.Errorf("could not determine if Ethereum client is syncing: %v\n", err)
		}
		if s {
			return fmt.Errorf("ethereum node is still sycning with the network")
		}
	}
	return nil
}

func AddDBToCtx(remote bool) error {
	cfg := config.GetConfig()
	//create a db instance
	os.RemoveAll(cfg.DBFile)
	DB, err := db.Open(cfg.DBFile)
	if err != nil {
		return err
	}

	var dataProxy db.DataServerProxy
	if remote {
		proxy, err := db.OpenRemoteDB(DB)
		if err != nil {
			log.Fatal(err)
		}
		dataProxy = proxy
	} else {
		proxy, err := db.OpenLocalProxy(DB)
		if err != nil {
			log.Fatal(err)
		}
		dataProxy = proxy
	}
	ctx = context.WithValue(ctx, tellorCommon.DataProxyKey, dataProxy)
	ctx = context.WithValue(ctx, tellorCommon.DBContextKey, DB)
	return nil
}

var GitTag string
var GitHash string

const versionMessage = `
    The official Tellor Miner %s (%s)
    -----------------------------------------
	Website: https://tellor.io
	Github:  https://github.com/tellor-io/TellorMiner
`

func App() *cli.Cli {

	app := cli.App("TellorMiner", "The tellor.io official miner")

	//app wide config options
	configPath := app.StringOpt("config", "config.json", "Path to the primary JSON config file")
	logPath := app.StringOpt("logConfig", "loggingConfig.json", "Path to a JSON logging config file")

	//this will get run before any of the commands
	app.Before = func() {
		ErrorHandler(config.ParseConfig(*configPath), "parsing config file")
		ErrorHandler(util.ParseLoggingConfig(*logPath), "parsing log file")
		ErrorHandler(buildContext(), "building context")
	}

	versionMessage := fmt.Sprintf(versionMessage, GitTag, GitHash)
	app.Version("version", versionMessage)

	app.Command("stake", "staking operations", stakeCmd)
	app.Command("transfer", "send TRB to address", moveCmd(ops.Transfer))
	app.Command("approve", "approve TRB to address", moveCmd(ops.Approve))
	app.Command("balance", "check balance of address", balanceCmd)
	app.Command("dispute", "dispute operations", disputeCmd)
	app.Command("mine", "mine for TRB", mineCmd)
	app.Command("dataserver", "start an independent dataserver", dataserverCmd)
	return app
}

func stakeCmd(cmd *cli.Cmd) {
	cmd.Command("deposit", "deposit TRB stake", simpleCmd(ops.Deposit))
	cmd.Command("withdraw", "withdraw TRB stake", simpleCmd(ops.WithdrawStake))
	cmd.Command("request", "request to withdraw TRB stake", simpleCmd(ops.RequestStakingWithdraw))
	cmd.Command("status", "show current staking status", simpleCmd(ops.ShowStatus))
}

func simpleCmd(f func(context.Context) error) func(*cli.Cmd) {
	return func(cmd *cli.Cmd) {
		cmd.Action = func() {
			ErrorHandler(f(ctx), "")
		}
	}
}

func moveCmd(f func(common.Address, *big.Int, context.Context) error) func(*cli.Cmd) {
	return func(cmd *cli.Cmd) {
		amt := TRBAmount{}
		addr := ETHAddress{}
		cmd.VarArg("AMOUNT", &amt, "amount to transfer")
		cmd.VarArg("ADDRESS", &addr, "ethereum public address")
		cmd.Action = func() {
			ErrorHandler(f(addr.addr, amt.Int, ctx), "move")
		}
	}
}

func balanceCmd(cmd *cli.Cmd) {
	addr := ETHAddress{}
	cmd.VarArg("ADDRESS", &addr, "ethereum public address")
	cmd.Spec = "[ADDRESS]"
	cmd.Action = func() {
		var zero [20]byte
		if bytes.Compare(addr.addr.Bytes(), zero[:]) == 0 {
			addr.addr = ctx.Value(tellorCommon.PublicAddress).(common.Address)
		}
		ErrorHandler(ops.Balance(ctx, addr.addr), "checking balance")
	}
}

func disputeCmd(cmd *cli.Cmd) {
	cmd.Command("vote", "vote on an active dispute", voteCmd)
	cmd.Command("new", "start a new dispute", newDisputeCmd)
	cmd.Command("show", "show existing disputes", simpleCmd(ops.List))
}

func voteCmd(cmd *cli.Cmd) {
	disputeID := EthereumInt{}
	cmd.VarArg("DISPUTE_ID", &disputeID, "dispute id")
	supports := cmd.BoolArg("SUPPORT", false, "do you support the dispute? (true|false)")
	cmd.Action = func() {
		ErrorHandler(ops.Vote(disputeID.Int, *supports, ctx), "vote")
	}
}

func newDisputeCmd(cmd *cli.Cmd) {
	requestID := EthereumInt{}
	timestamp := EthereumInt{}
	minerIndex := EthereumInt{}
	cmd.VarArg("REQUEST_ID", &requestID, "request id")
	cmd.VarArg("TIMESTAMP", &timestamp, "timestamp")
	cmd.VarArg("MINER_INDEX", &minerIndex, "miner to dispute (0-4)")
	cmd.Action = func() {
		ErrorHandler(ops.Dispute(requestID.Int, timestamp.Int, minerIndex.Int, ctx), "new dipsute")
	}
}

func mineCmd(cmd *cli.Cmd) {
	remoteDS := cmd.BoolOpt("remote r", false, "connect to remote dataserver")
	cmd.Action = func() {
		//create os kill sig listener
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt)
		exitChannels := make([]*chan os.Signal, 0)

		cfg := config.GetConfig()
		var ds *ops.DataServerOps
		if !cfg.EnablePoolWorker {
			ErrorHandler(AddDBToCtx(*remoteDS), "initializing database")
			if !*remoteDS {
				ch := make(chan os.Signal)
				exitChannels = append(exitChannels, &ch)

				var err error
				ds, err = ops.CreateDataServerOps(ctx, ch)
				if err != nil {
					log.Fatal(err)
				}
				//start and wait for it to be ready
				ds.Start(ctx)
				<-ds.Ready()
			}
		}
		//start miner
		ch := make(chan os.Signal)
		exitChannels = append(exitChannels, &ch)
		miner, err := ops.CreateMiningManager(ctx, ch, ops.NewSubmitter())
		if err != nil {
			log.Fatal(err)
		}
		miner.Start(ctx)

		//now we wait for kill sig
		<-c
		//and then notify exit channels
		for _, ch := range exitChannels {
			*ch <- os.Interrupt
		}
		cnt := 0
		start := time.Now()
		for {
			cnt++
			dsStopped := false
			minerStopped := false

			if ds != nil {
				dsStopped = !ds.Running
			} else {
				dsStopped = true
			}

			if miner != nil {
				minerStopped = !miner.Running
			} else {
				minerStopped = true
			}

			if !dsStopped && !minerStopped && cnt > 60 {
				fmt.Printf("Taking longer than expected to stop operations. Waited %v so far\n", time.Now().Sub(start))
			} else if dsStopped && minerStopped {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Printf("Main shutdown complete\n")
	}
}

func dataserverCmd(cmd *cli.Cmd) {
	cmd.Action = func() {
		//create os kill sig listener
		c := make(chan os.Signal)
		signal.Notify(c, os.Interrupt)

		var ds *ops.DataServerOps
		ErrorHandler(AddDBToCtx(true), "initializing database")
		ch := make(chan os.Signal)
		var err error
		ds, err = ops.CreateDataServerOps(ctx, ch)
		if err != nil {
			log.Fatal(err)
		}
		//start and wait for it to be ready
		ds.Start(ctx)
		<-ds.Ready()

		//now we wait for kill sig
		<-c
		//and then notify exit channels
		ch <- os.Interrupt

		cnt := 0
		start := time.Now()
		for {
			cnt++
			dsStopped := false

			if ds != nil {
				dsStopped = !ds.Running
			} else {
				dsStopped = true
			}

			if !dsStopped && cnt > 60 {
				fmt.Printf("Taking longer than expected to stop operations. Waited %v so far\n", time.Now().Sub(start))
			} else if dsStopped {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Printf("Main shutdown complete\n")
	}

}

func main() {
	//see, programming is easy. Just create an App() and run it!!!!!
	app := App()
	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "app.Run failed: %v\n", err)
	}
}
