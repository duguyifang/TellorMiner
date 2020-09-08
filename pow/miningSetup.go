package pow

import (
	"fmt"

	"github.com/tellor-io/TellorMiner/config"
)

func SetupMiningGroup(cfg *config.Config) (*MiningGroup, error) {
	var hashers []Hasher
	fmt.Printf("No GPUs enabled, falling back to CPU mining, using %d threads\n", cfg.NumProcessors)
	for i := 0; i < cfg.NumProcessors; i++ {
		hashers = append(hashers, NewCpuMiner(int64(i)))
	}

	return NewMiningGroup(hashers), nil
}
