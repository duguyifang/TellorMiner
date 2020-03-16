package util

import (
	"sync"
)
var (
	messagehandle *MessageChanel
	once sync.Once
)

type MessageChanel struct {
	UpdateChallenge   chan string
	UpdateTx          chan string
	IsReady           bool
}

func GetInstance() *MessageChanel {
    once.Do(func() {
        messagehandle = &MessageChanel {
			UpdateChallenge : make(chan string, 1),
			UpdateTx        : make(chan string, 1),
			IsReady         : false,
		}
    })
    return messagehandle
}