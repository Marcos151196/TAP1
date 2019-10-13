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
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
var msgReceived = make(chan *sqs.Message, 1)
var msgProcessed = make(chan bool, 1)

func main() {
	initConfig()
	keyboardInterr := make(chan os.Signal, 1)
	signal.Notify(keyboardInterr, os.Interrupt, syscall.SIGTERM)

	// RECEIVE MSGS
	go func() {
		for {
			RXmsg := &sqs.ReceiveMessageInput{
				MessageAttributeNames: aws.StringSlice([]string{"clientName", "timestamp", "sessionID", "cmd"}),
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

			msgReceived <- resultRX.Messages[0]
			<-msgProcessed

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
			msg := <-msgReceived
			cmd := *msg.MessageAttributes["cmd"].StringValue
			sessID := *msg.MessageAttributes["sessionID"].StringValue
			clientName := *msg.MessageAttributes["clientName"].StringValue
			timestamp := *msg.MessageAttributes["timestamp"].StringValue
			log.Infof("New message received. Client: %s\tCommand: %s", clientName, cmd)
			cRX, _ := strconv.Atoi(cmd)
			// ECHO
			if cRX == 1 {
				text := *msg.Body
				if text == "END" {
					log.Infof("End of conversation with %s", clientName)
					err := UploadFileToS3(clientName)
					if err != nil {
						log.Errorf("Could not upload conversation to S3: %v", err)
					}
					DeleteTemporalConversation(clientName)
					DeleteTemporalConversation(clientName + "_new")
					msgProcessed <- true
				} else {
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
							"timestamp": &sqs.MessageAttributeValue{
								DataType:    aws.String("String"),
								StringValue: aws.String(timestamp),
							},
							"cmd": &sqs.MessageAttributeValue{
								DataType:    aws.String("String"),
								StringValue: aws.String(cmd),
							},
						},
						MessageBody: aws.String(text),
						QueueUrl:    &outboxURL,
					}
					StoreNewLine(clientName, text, timestamp)
					log.Infof("Echoing message. Client: %s\tContent: %s", clientName, text)
					result, err := svc.SendMessage(msgTX)
					if err != nil {
						log.Errorf("Could not send message to SQS queue: %v", err)
					} else {
						log.Infof("Message sent to SQS. MessageID: %v", *result.MessageId)
					}
					msgProcessed <- true
				}
			} else if cRX == 2 { // SEARCH

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

func DownloadConversation(client string) error {

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)
	filename := fmt.Sprintf("tmpConv/%s.txt", client)
	// Create a file to write the S3 Object contents to.
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("Failed to create file %q, %v", filename, err)
	}

	downloadPath := fmt.Sprintf("%s/%s.txt", viper.GetString("s3.conversationspath"), client)
	// Write the contents of S3 Object to the file
	n, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(viper.GetString("s3.bucketname")),
		Key:    aws.String(downloadPath),
	})
	if err != nil {
		return fmt.Errorf("failed to download file, %v", err)
	}
	log.Infof("File %s_old.txt downloaded, %d bytes.", client, n)
	return nil
}

func StoreNewLine(client string, body string, timestamp string) error {
	f, err := os.OpenFile(fmt.Sprintf("tmpConv/%s_new.txt", client), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Could not open/create file: %v", err)
		return err
	}

	_, err = f.WriteString(fmt.Sprintf("%s|||%s\n", timestamp, body))
	if err != nil {
		log.Warnf("Could not write new line to file: %v", err)
		f.Close()
		return err
	}

	log.Debugf("New line written succesfully!")
	err = f.Close()
	if err != nil {
		log.Errorf("Could not save and close file: %v", err)
		return err
	}

	return nil
}

func DeleteTemporalConversation(client string) error {
	err := os.Remove(fmt.Sprintf("tmpConv/%s.txt", client))

	if err != nil {
		log.Errorf("Could not delete temporal conversation file for client %s: %v", client, err)
		return err
	}
	return nil
}

// UploadFileToS3 saves a file to aws bucket and returns the url to the file and an error if there's any
func UploadFileToS3(client string) error {

	newFileName := "tmpConv/" + client + "_new.txt"
	oldFileName := "tmpConv/" + client + ".txt"
	err := DownloadConversation(client)
	old, err := os.OpenFile(oldFileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Could not open old file for writing: %v", err)
	}
	defer old.Close()

	new, err := os.Open(newFileName)
	if err != nil {
		return fmt.Errorf("Failed to open new file for reading: %v", err)
	}
	defer new.Close()

	n, err := io.Copy(old, new)
	if err != nil {
		return fmt.Errorf("Failed to append new file to old: %v", err)
	}
	log.Infof("Wrote %d bytes of %s to the end of %s\n", n, newFileName, oldFileName)

	new.Close()
	old.Close()

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)
	filename := fmt.Sprintf("tmpConv/%s.txt", client)
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Failed to open file %q, %v", filename, err)
	}

	uploadPath := fmt.Sprintf("%s/%s.txt", viper.GetString("s3.conversationspath"), client)
	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(viper.GetString("s3.bucketname")),
		Key:    aws.String(uploadPath),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("Failed to upload file, %v", err)
	}
	log.Infof("File uploaded to, %s\n", uploadPath)
	return nil
}
