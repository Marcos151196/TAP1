package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	aws "github.com/aws/aws-sdk-go/aws"
	session "github.com/aws/aws-sdk-go/aws/session"
	sqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	viper "github.com/theherk/viper"
)

var logFile, verboseLevel, inboxURL, outboxURL string
var stdoutEnabled, fileoutEnabled bool
var cfgFile string = "config/config.toml"
var sess *session.Session = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))
var svc *sqs.SQS = sqs.New(sess)
var RXecho = make(chan *sqs.Message, 1)
var msgSent = make(chan bool, 1)

func main() {
	initConfig()
	keyboardInterr := make(chan os.Signal, 1)
	signal.Notify(keyboardInterr, os.Interrupt, syscall.SIGTERM)

	// RECEIVE MSGS
	go func() {
		for {
			RXmsg := &sqs.ReceiveMessageInput{
				MessageAttributeNames: aws.StringSlice([]string{"clientName", "sessionID", "cmd"}),
				QueueUrl:              &inboxURL,
				MaxNumberOfMessages:   aws.Int64(1),
				WaitTimeSeconds:       aws.Int64(1),
			}

			// READ MSG
			resultRX, err := svc.ReceiveMessage(RXmsg)
			if err != nil {
				log.Errorf("Error while receiving message: %v", err)
				continue
			}

			if len(resultRX.Messages) == 0 {
				continue
			}

			RXecho <- resultRX.Messages[0]
			<-msgSent

			// DELETE MSG
			_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      &inboxURL,
				ReceiptHandle: resultRX.Messages[0].ReceiptHandle,
			})

			if err != nil {
				log.Errorf("Error when trying to delete message after processing: %v", err)
				continue
			}

		}
	}()

	// PROCESS MSG
	go func() {
		for {
			msg := <-RXecho
			text := *msg.Body
			clientName := *msg.MessageAttributes["clientName"].StringValue
			sessID := *msg.MessageAttributes["sessionID"].StringValue
			cmd := *msg.MessageAttributes["cmd"].StringValue
			log.Infof("New message received. Client: %s\tContent: %s", clientName, text)
			c, _ := strconv.Atoi(cmd)
			if c == 1 {
				msgTX := &sqs.SendMessageInput{
					MessageAttributes: map[string]*sqs.MessageAttributeValue{
						"clientName": &sqs.MessageAttributeValue{
							DataType:    aws.String("String"),
							StringValue: aws.String(clientName),
						},
						"sessionID": &sqs.MessageAttributeValue{
							DataType:    aws.String("String"),
							StringValue: aws.String(sessID),
						},
						"cmd": &sqs.MessageAttributeValue{
							DataType:    aws.String("String"),
							StringValue: aws.String(cmd),
						},
					},
					MessageBody: aws.String(text),
					QueueUrl:    &outboxURL,
				}

				log.Infof("Echoing message. Client: %s\tContent: %s", clientName, text)
				result, err := svc.SendMessage(msgTX)
				if err != nil {
					log.Errorf("Could not send message to SQS queue: %v", err)
				} else {
					log.Infof("Message sent to SQS. MessageID: %v", *result.MessageId)
				}
				msgSent <- true
			}
		}
	}()
	<-keyboardInterr

}

func initConfig() {
	// CONFIG FILE
	viper.SetConfigFile(cfgFile)
	if err := viper.ReadInConfig(); err != nil {
		log.Errorf("[INIT] Unable to read config from file %s: %v", cfgFile, err)
		os.Exit(1)
	} else {
		log.Infof("[INIT] Read configuration from file %s", cfgFile)
	}

	// LOGGING SETTINGS
	logFile = fmt.Sprintf("%s/logs.log", viper.GetString("log.logfilepath"))
	stdoutEnabled = viper.GetBool("log.stdout")
	fileoutEnabled = viper.GetBool("log.fileout")
	verboseLevel = strings.ToLower(viper.GetString("log.level"))
	if stdoutEnabled && fileoutEnabled {
		f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Warnf("[INIT] Unable to open logfile (%s): %v", logFile, err)
		} else {
			log.Infof("Using logfile %s", logFile)
			mw := io.MultiWriter(os.Stdout, f)
			log.SetOutput(mw)
		}

	} else if stdoutEnabled {
		mw := io.MultiWriter(os.Stdout)
		log.SetOutput(mw)
	} else if fileoutEnabled {
		f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Warnf("[INIT] Unable to open logfile (%s): %v", logFile, err)
		} else {
			log.Infof("Using logfile %s", logFile)
			mw := io.MultiWriter(f)
			log.SetOutput(mw)
		}
	} else {
		log.SetOutput(ioutil.Discard)
	}

	log.SetLevel(log.PanicLevel)
	if verboseLevel == "debug" {
		log.SetLevel(log.DebugLevel)
	} else if verboseLevel == "info" {
		log.SetLevel(log.InfoLevel)
	} else if verboseLevel == "warning" {
		log.SetLevel(log.WarnLevel)
	} else if verboseLevel == "error" {
		log.SetLevel(log.ErrorLevel)
	}
	if viper.GetBool("log.jsonformat") {
		log.Info("[INIT] Use JSON log formatter with full timestamp")
		log.SetFormatter(&log.JSONFormatter{})
	}

	// START SQS INBOX AND OUTBOX QUEUES
	inboxURL = viper.GetString("sqs.inboxURL")
	outboxURL = viper.GetString("sqs.outboxURL")

	return
}
