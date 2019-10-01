package gorbm

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis"
	jsoniter "github.com/json-iterator/go"
	uuid "github.com/satori/go.uuid"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// StrRequest structure of request send to Queue
type StrRequest struct {
	GUID        uuid.UUID
	TimeStamp   time.Time
	TokenString string
	Content     interface{}
}

// GoRbm stores the operating information
type GoRbm struct {
	rOption        redis.Options
	rClient        *redis.Client
	err            error
	workerID       string
	eventQueueName string
}

// NewRBM return
func NewRBM(w string) *GoRbm {
	return &GoRbm{eventQueueName: w}
}

// PushMessage push message in redis
func (gorbm *GoRbm) PushMessage(tokenString string, content interface{}) uuid.UUID {

	var message StrRequest
	var ret []byte

	message.GUID, gorbm.err = uuid.NewV4()
	if gorbm.err == nil {
		message.TimeStamp = time.Now()
		message.Content = content
		message.TokenString = tokenString
		ret, gorbm.err = jsoniter.Marshal(message)
		if gorbm.err == nil {
			gorbm.err = gorbm.rClient.LPush(gorbm.eventQueueName, string(ret)).Err()
		}
	}
	return message.GUID
}

// Listen eventQueueName,  and call the callback function
func (gorbm *GoRbm) Listen(workerID string, callBack func(message interface{}) interface{}) {
	var processingQueue string
	processingQueue = gorbm.eventQueueName + "-processing-" + workerID
	gorbm.switchToProcessingQueue(processingQueue)
	gorbm.threatProcessingQueue(processingQueue, callBack)
}

func (gorbm *GoRbm) switchToProcessingQueue(processingQueue string) {
	var retour string
	for ok := true; ok; ok = (retour != "") {
		retour = gorbm.rClient.RPopLPush(gorbm.eventQueueName, processingQueue).Val()
	}
}

func (gorbm *GoRbm) threatProcessingQueue(processingQueue string, callback func(message interface{}) interface{}) {
	var retour string
	var message StrRequest
	var resultatCallBack interface{}
	var guid string

	for ok := true; ok; ok = (retour != "") {

		retour = gorbm.rClient.RPop(processingQueue).Val()
		if retour != "" {
			byt := []byte(retour)
			gorbm.err = jsoniter.Unmarshal(byt, &message)
			if gorbm.err == nil {
				guid = message.GUID.String()
				gorbm.err = gorbm.rClient.Set("InProgress:"+guid, retour, 0).Err()
				if gorbm.err == nil {
					resultatCallBack = callback(message.Content)
					gorbm.err = gorbm.rClient.Del("InProgress:" + guid).Err()
					if gorbm.err == nil {
						gorbm.err = gorbm.rClient.Set("Done:"+guid, resultatCallBack, 0).Err()
						if gorbm.err == nil {
							t := time.Now().Add(7 * 24 * time.Hour)
							gorbm.rClient.ExpireAt("Done:"+guid, t)
						}
					}
				}
			}
		}
	}
}

// GetStatus retrieve the message once the processing is complete
func (gorbm *GoRbm) GetStatus(GUID string) (interface{}, error) {
	var responses []string
	var status string
	var err error
	responses, gorbm.err = gorbm.rClient.Keys("Done:" + GUID).Result()
	if gorbm.err == nil {
		l := len(responses)
		switch {
		case l == 1:
			// * Travail terminé, récupération du résultat
			status = strings.Split(responses[0], ":")[0]
		case l == 0:
			responses, gorbm.err = gorbm.rClient.Keys("InProgress:" + GUID).Result()
			if gorbm.err == nil {
				l := len(responses)
				switch {
				case l == 1:
					// * Travail dans InProgress
					status = "InProgress"
				case l == 0:
					// ? Travail toujours en cours, y a t'il un problème
					status = "Nothing"
				case l > 1:
					status = "Error"
					err = fmt.Errorf("Duplicate in InProgress")
				}
			}
		case l > 1:
			status = "Error"
			err = fmt.Errorf("Duplicate in Done")
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
	gorbm.rOption.DB = 2
}

// Disconnect close redis connection
func (gorbm *GoRbm) Disconnect() {
	gorbm.rClient.Close()
}

// GetError returns an error resulted from Go-RBM.
func (gorbm *GoRbm) GetError() error {
	return gorbm.err
}
