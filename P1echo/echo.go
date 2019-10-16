package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	aws "github.com/aws/aws-sdk-go/aws"
	session "github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	s3manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
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
var sqssvc *sqs.SQS = sqs.New(sess)

func main() {
	initConfig() // Set config file, logs and queues URLs

	// MAIN LOOP (RECEIVE, PROCESS AND DELETE IF PROCESSED, IF NOT GO BACK TO RECEIVE)
	for {
		RXmsg := &sqs.ReceiveMessageInput{
			MessageAttributeNames: aws.StringSlice([]string{"clientName", "timestamp", "sessionID", "cmd"}),
			QueueUrl:              &inboxURL,
			MaxNumberOfMessages:   aws.Int64(1),
			WaitTimeSeconds:       aws.Int64(1),
		}

		resultRX, err := sqssvc.ReceiveMessage(RXmsg)
		if err != nil {
			log.Errorf("Error while receiving message: %v", err)
			continue
		}

		if len(resultRX.Messages) == 0 {
			continue
		}

		err = ProcessRXMessage(resultRX.Messages[0])
		if err != nil {
			continue
		}

		// Delete message after processing it
		_, err = sqssvc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &inboxURL,
			ReceiptHandle: resultRX.Messages[0].ReceiptHandle,
		})

		if err != nil {
			log.Errorf("Error when trying to delete message after processing: %v", err)
		}
	}

}

// SETS CONFIG FILE, LOGS ETC
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

	// GET SQS INBOX AND OUTBOX QUEUES URL
	inboxURL = viper.GetString("sqs.inboxURL")
	outboxURL = viper.GetString("sqs.outboxURL")

	return
}

// CHECK IF MSG IS FOR ECHO APP, CHECK THAT IT'S NOT END, STORE IT IN S3 AND SEND IT BACK THROUGH OUTBOX QUEUE
// RETURNS ERROR IF THE MESSAGE WAS NOT FOR THE ECHO APP
func ProcessRXMessage(msg *sqs.Message) error {
	cmd := *msg.MessageAttributes["cmd"].StringValue
	sessID := *msg.MessageAttributes["sessionID"].StringValue
	clientName := *msg.MessageAttributes["clientName"].StringValue
	timestamp := *msg.MessageAttributes["timestamp"].StringValue
	log.Infof("New message received. Client: %s\tCommand: %s", clientName, cmd)
	cRX, _ := strconv.Atoi(cmd)
	if cRX == 1 {
		text := *msg.Body
		if text == "END" {
			log.Infof("End of conversation with %s", clientName)
			return nil
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
			err := StoreNewLine(clientName, sessID, text, timestamp)
			if err != nil {
				log.Errorf("Could not upload conversation to S3: %v", err)
			}
			DeleteTemporalConversation(clientName + "_" + sessID)
			log.Infof("Echoing message. Client: %s\tContent: %s", clientName, text)
			result, err := sqssvc.SendMessage(msgTX)
			if err != nil {
				log.Errorf("Could not send message to SQS queue: %v", err)
			} else {
				log.Infof("Message sent to SQS. MessageID: %v", *result.MessageId)
			}
		}
	} else {
		return fmt.Errorf("This message was not for the echo app.")
	}
	return nil
}

// DOWNLOAD FILE FROM S3 AND STORE IT IN LOCAL FOLDER tmpConv
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

// DOWNLOAD [CLIENT]_[SESSION_ID].txt FILE FROM S3, APPEND NEW LINE AND UPLOAD IT AGAIN TO S3
func StoreNewLine(client string, sessID string, body string, timestamp string) error {
	err := DownloadConversation(client + "_" + sessID)
	if err != nil {
		log.Warnf("Could not download document before adding new line: %v", err)
	}
	f, err := os.OpenFile(fmt.Sprintf("tmpConv/%s.txt", client+"_"+sessID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Could not open/create file: %v", err)
	}

	_, err = f.WriteString(fmt.Sprintf("%s|||%s\n", timestamp, body))
	if err != nil {
		f.Close()
		return fmt.Errorf("Could not write new line to file: %v", err)
	}
	log.Infof("New line written succesfully!")
	f.Close()

	fileName := fmt.Sprintf("tmpConv/%s.txt", client+"_"+sessID)
	f, err = os.Open(fileName)
	if err != nil {
		return fmt.Errorf("Failed to open file %q, %v", fileName, err)
	}
	err = UploadFileToS3(client, f, sessID)
	if err != nil {
		f.Close()
		return fmt.Errorf("Could not upload file to S3: %v", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("Could not save and close file: %v", err)
	}

	return nil
}

// DELETE MESSAGE FROM SQS QUEUE AFTER PROCESSING IT
func DeleteTemporalConversation(client string) error {
	err := os.Remove(fmt.Sprintf("tmpConv/%s.txt", client))

	if err != nil {
		log.Errorf("Could not delete temporal conversation file for client %s: %v", client, err)
		return err
	}
	return nil
}

// UPLOAD FILE TO S3. BUCKET AND PATH ARE SPECIFIEND IN THE CONFIG FILE
func UploadFileToS3(client string, f *os.File, sessID string) error {
	uploadPath := fmt.Sprintf("%s/%s.txt", viper.GetString("s3.conversationspath"), client+"_"+sessID)
	uploader := s3manager.NewUploader(sess)
	_, err := uploader.Upload(&s3manager.UploadInput{
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
