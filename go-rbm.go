package gorbm

import (
	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// NewRBM return
func NewRBM(w string) *GoRbm {
	return &GoRbm{workerID: w}
}

// GoRbm stores the operating information
type GoRbm struct {
	rOption  redis.Options
	rClient  *redis.Client
	optLoad  bool
	err      error
	workerID string
}

// GetError returns an error resulted from Go-RBM.
func (gorbm *GoRbm) GetError() error {
	return gorbm.err
}

// GetWorkerID returns WorkerID.
func (gorbm *GoRbm) GetWorkerID() string {
	return gorbm.workerID
}

// SetWorkerID Change the ccurrent WorkerID
func (gorbm *GoRbm) SetWorkerID(workerID string) {
	gorbm.workerID = workerID
}

// PushMessage push message in redis
func (gorbm *GoRbm) PushMessage(message string) {

	// TODO : mettre une vérification que le GUID n'est pas déjà fait l'ojet d'une demande
	gorbm.err = gorbm.rClient.RPush(gorbm.workerID, message).Err()

}

// Listen the liste on workerID and call the callback function
func (gorbm *GoRbm) Listen(callBack func(message string)) {

}

// GetStatus retrieve the message once the processing is complete
func (gorbm *GoRbm) GetStatus(GUID string) string {

	var messages []string

	messages, gorbm.err = gorbm.rClient.Keys("*" + GUID).Result()

	if gorbm.err == nil {
		// TODO : faire un substring de la chaine pour vérifier dans quel état est le traitment

		return messages[0]
	}

	return ""

}

// Connect Establish connection
func (gorbm *GoRbm) Connect() {

	gorbm.loadOption()
	if gorbm.err == nil {

		client := redis.NewClient(&redis.Options{
			Addr:     gorbm.rOption.Addr,
			Password: gorbm.rOption.Password, // no password set
			DB:       gorbm.rOption.DB,       // use default DB
		})

		_, gorbm.err = client.Ping().Result()
		if gorbm.err == nil {
			gorbm.rClient = client
		}
	}

}

func (gorbm *GoRbm) loadOption() {
	gorbm.rOption.Addr = "127.0.0.1:6379"
	gorbm.rOption.Password = ""
	gorbm.rOption.DB = 3
}

// Disconnect close redis connection
func (gorbm *GoRbm) Disconnect() {
	gorbm.rClient.Close()
}
