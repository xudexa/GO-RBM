package gorbm

import (
	"fmt"
	"strings"

	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// NewRBM return
func NewRBM(w string) *GoRbm {
	return &GoRbm{eventQueueName: w}
}

// GoRbm stores the operating information
type GoRbm struct {
	rOption        redis.Options
	rClient        *redis.Client
	err            error
	workerID       string
	eventQueueName string
}

// PushMessage push message in redis
func (gorbm *GoRbm) PushMessage(message string) {

	// TODO : mettre une vérification que le GUID n'est pas déjà fait l'ojet d'une demande
	gorbm.err = gorbm.rClient.LPush(gorbm.eventQueueName, message).Err()

}

// Listen eventQueueName,  and call the callback function
func (gorbm *GoRbm) Listen(workerID string, callBack func(message string)) {

	var processingQueue string
	processingQueue = gorbm.eventQueueName + "-processing-" + workerID
	gorbm.switchToProcessingQueue(processingQueue)

}

func (gorbm *GoRbm) switchToProcessingQueue(processingQueue string) {
	retour, err := gorbm.rClient.RPopLPush(gorbm.eventQueueName, processingQueue).Result()
	if err == nil {
		fmt.Println(retour)
	} else {
		fmt.Println(err)
		fmt.Println(retour)
	}

}

// GetStatus retrieve the message once the processing is complete
func (gorbm *GoRbm) GetStatus(GUID string) (string, error) {

	var responses []string
	var status string
	var err error

	responses, gorbm.err = gorbm.rClient.Keys("Done" + GUID).Result()
	if gorbm.err == nil {

		l := len(responses)
		switch {
		case l == 1:
			// * Travail terminé, récupération du résultat
			status = strings.Split(responses[0], ":")[0]

		case l == 0:
			responses, gorbm.err = gorbm.rClient.Keys("InProgress" + GUID).Result()
			if gorbm.err == nil {
				l := len(responses)
				switch {
				case l == 1:
					// * Travail dans InProgress
					status = "InProgress"
				case l == 0:
					// ? Travail toujours en cours, y a t'il un problème
					status = "nothing"
				case l > 1:
					status = "Error"
					err = fmt.Errorf("Doublons dans le InProgress")
				}

			}

		case l > 1:
			status = "Error"
			err = fmt.Errorf("Doublons dans le Done")

		}
	}

	return status, err

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

// GetError returns an error resulted from Go-RBM.
func (gorbm *GoRbm) GetError() error {
	return gorbm.err
}

/*
// GetWorkerID returns WorkerID.
func (gorbm *GoRbm) GetWorkerID() string {
	return gorbm.workerID
}

// SetWorkerID Change the ccurrent WorkerID
func (gorbm *GoRbm) SetWorkerID(workerID string) {
	gorbm.workerID = workerID
}
*/
