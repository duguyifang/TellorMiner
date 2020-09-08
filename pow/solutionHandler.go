package pow

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	tellorCommon "github.com/tellor-io/TellorMiner/common"
	"github.com/tellor-io/TellorMiner/config"
	"github.com/tellor-io/TellorMiner/db"
	"github.com/tellor-io/TellorMiner/util"
	"io/ioutil"
	"math/big"
	"os"
	"strconv"
	"strings"
)

/**
* The solution handler has one purpose: to either submit the solution on-chain
* or to reject it if the miner has already submitted a solution for the challenge
* or the the solution's challenge does not match current challenge
 */

type SolutionHandler struct {
	log              *util.Logger
	pubKey           string
	proxy            db.DataServerProxy
	currentChallenge *MiningChallenge
	currentNonce     string
	currentValue     *big.Int
	submitter        tellorCommon.TransactionSubmitter
}

func CreateSolutionHandler(
	cfg *config.Config,
	submitter tellorCommon.TransactionSubmitter,
	proxy db.DataServerProxy) *SolutionHandler {

	//get address from config
	_fromAddress := cfg.PublicAddress

	//convert to address
	fromAddress := common.HexToAddress(_fromAddress)
	pubKey := strings.ToLower(fromAddress.Hex())

	return &SolutionHandler{
		pubKey:    pubKey,
		proxy:     proxy,
		submitter: submitter,
		log:       util.NewLogger("pow", "SolutionHandler"),
	}
}

func (s *SolutionHandler) Submit(ctx context.Context, result *Result) {
	challenge := result.Work.Challenge
	nonce := result.Nonce
	manualVal := int64(0)
	valKey := fmt.Sprintf("%s%d", db.QueriedValuePrefix, challenge.RequestID.Uint64())
	s.log.Info("Getting pending txn and value from data server...")
	m, err := s.proxy.BatchGet([]string{db.CurrentChallengeKey, db.RequestIdKey, valKey})

	if err != nil {
		s.log.Error("Problem reading pending txn: %v", err)
		return
	}
	s.log.Debug("Retrieved data from data server %v", m)

	val := m[valKey]
	if val == nil || len(val) == 0 {
		if challenge.RequestID.Uint64() > 53 && (val == nil || len(val) == 0) {
			s.log.Error("Have not retrieved price data for requestId %d. WARNING: Submitting 0 because of faulty API request", challenge.RequestID.Uint64())
		} else {
			jsonFile, err := os.Open("manualData.json")
			if err != nil {
				fmt.Println(err)
			}
			defer jsonFile.Close()
			byteValue, _ := ioutil.ReadAll(jsonFile)
			var result map[string]map[string]int64
			json.Unmarshal([]byte(byteValue), &result)
			_id := strconv.FormatUint(challenge.RequestID.Uint64(), 10)
			manualVal = result[_id]["VALUE"]
			if manualVal == 0 {
				s.log.Error("No Value in database, not submitting.")
				return
			} else {
				fmt.Println("Using Manually entered value: ", manualVal)
			}
		}
	}

	value, err := hexutil.DecodeBig(string(val))
	if err != nil {
		if challenge.RequestID.Uint64() > 51 {
			s.log.Error("Problem decoding price value prior to submitting solution: %v\n", err)
			if len(val) == 0 {
				s.log.Error("0 value being submitted")
				value = big.NewInt(0)
			}
		} else if manualVal > 0 {
			value = big.NewInt(manualVal)
		} else {
			s.log.Error("No Value in database, not submitting.")
			return
		}
	}

	s.currentChallenge = challenge
	s.currentNonce = nonce
	s.currentValue = value

	s.log.Info("Submitting solution to contract...")
	err = s.submitter.PrepareTransaction(ctx, s.proxy, "submitSolution", s.submit)
	if err != nil {
		s.log.Error("Problem submitting txn", err)
	} else {
		s.log.Info("Successfully submitted solution")
	}
}

func (s *SolutionHandler) submit(ctx context.Context, contract tellorCommon.ContractInterface) (*types.Transaction, error) {

	txn, err := contract.SubmitSolution(
		s.currentNonce,
		s.currentChallenge.RequestID,
		s.currentValue)
	if err != nil {
		s.log.Error("Problem submitting solution: %v", err)
		return txn, err
	}

	return txn, err
}
