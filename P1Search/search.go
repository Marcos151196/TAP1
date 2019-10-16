package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

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
var sqssvc *sqs.SQS = sqs.New(sess)
var s3svc *s3.S3 = s3.New(sess)

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

		// READ MSG
		resultRX, err := sqssvc.ReceiveMessage(RXmsg)
		if err != nil {
			log.Errorf("Error while receiving message: %v", err)
			continue
		}

		if len(resultRX.Messages) == 0 {
			continue
		}

		// Process message
		err = ProcessRXMessage(resultRX.Messages[0])
		if err != nil {
			log.Errorf("Could not process message: %v", err)
			continue
		}

		// Delete message after processing it
		_, err = sqssvc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &inboxURL,
			ReceiptHandle: resultRX.Messages[0].ReceiptHandle,
		})

		if err != nil {
			log.Errorf("Error when trying to delete message after processing: %v", err)
			continue
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

// CHECK IF MSG IS FOR SEARCH APP, DOWNLOAD ALL SESSIONS FOR THAT CLIENT, FILTER THEM USING THE KEY SENTENCE
// AND COMBINE THEM IN ONE FILE BEFORE SENDING IT TO THE CLIENT THROUGH OUTBOX QUEUE. REMOVES ALL TEMPORAL FILES AFTER SENDING TO SQS.
// RETURNS ERROR IF THE MESSAGE WAS NOT FOR THE SEARCH APP OR SOMETHING WENT WRONG
func ProcessRXMessage(msg *sqs.Message) error {
	cmd := *msg.MessageAttributes["cmd"].StringValue
	sessID := *msg.MessageAttributes["sessionID"].StringValue
	clientName := *msg.MessageAttributes["clientName"].StringValue
	timestamp := *msg.MessageAttributes["timestamp"].StringValue
	log.Infof("New message received. Client: %s\tCommand: %s", clientName, cmd)
	cRX, _ := strconv.Atoi(cmd)
	// SEARCH
	if cRX == 2 {
		text := *msg.Body
		err := DownloadConversation(clientName)
		if err != nil {
			return fmt.Errorf("Could not download conversation %v", err)
		}
		filtFileName, err := CreateFilteredConversationFile(clientName, text)
		if err != nil {
			return fmt.Errorf("Could not filter file: %v", err)
		}

		filtFile, err := ioutil.ReadFile(filtFileName)
		if err != nil {
			return fmt.Errorf("Could not read filtered file before sending it to SQS: %v", err)
		}
		filtFileStr := string(filtFile)
		if filtFileStr == "" {
			filtFileStr = "EMPTY CONVERSATION"
		}

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
			MessageBody: aws.String(filtFileStr),
			QueueUrl:    &outboxURL,
		}
		DeleteTemporalConversation(clientName + "_filtered")
		log.Infof("Sending filtered conversation to %s", clientName)
		result, err := sqssvc.SendMessage(msgTX)
		if err != nil {
			log.Errorf("Could not send message to SQS queue: %v", err)
		} else {
			log.Infof("Message sent to SQS. MessageID: %v", *result.MessageId)
			newFileName := viper.GetString("s3.conversationspath") + "/" + clientName + ".txt"
			os.Remove(newFileName)
		}
	} else {
		return fmt.Errorf("This message was not for the search app.")
	}
	return nil
}

// DOWNLOAD FILE FROM S3 AND STORE IT IN LOCAL FOLDER conversations
func DownloadConversation(client string) error {
	bucketname := viper.GetString("s3.bucketname")
	// List all sessions for the client stored in S3
	resp, err := s3svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketname), Prefix: aws.String(viper.GetString("s3.conversationspath") + "/" + client)})
	if err != nil {
		return fmt.Errorf("Unable to list items in bucket %q, %v", bucketname, err)
	}

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)
	// Iterate over the session list for the client and download them to local path conversations
	for _, item := range resp.Contents {
		downloadPath := *item.Key
		// Create a file to write the S3 Object contents to.
		f, err := os.OpenFile(downloadPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Errorf("Failed to create file %q, %v", downloadPath, err)
			continue
		}

		// Write the contents of S3 Object to the file
		_, err = downloader.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(bucketname),
			Key:    aws.String(downloadPath),
		})
		f.Close()
		if err != nil {
			os.Remove(downloadPath)
			log.Errorf("Failed to download file %s, %v", downloadPath, err)
		}
	}

	// Combine all sessions in one file and remove them
	err = CombineSessionsToFile(client, resp)
	if err != nil {
		return fmt.Errorf("Could not combine sessions to file: %v", err)
	}
	log.Infof("Whole conversation of client %s has been downloaded.", client)
	return nil
}

// COMBINES ALL LOCAL (ALREADY DOWNLOADED) SESSIONS FOR A CLIENT IN ONE FILE AND DELETES THEM WHEN DONE, LEAVING ONLY THE COMBINED FILE
func CombineSessionsToFile(client string, items *s3.ListObjectsV2Output) error {
	newFileName := viper.GetString("s3.conversationspath") + "/" + client + ".txt"
	os.Remove(newFileName)
	newFile, err := os.OpenFile(newFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	defer newFile.Close()
	if err != nil {
		return fmt.Errorf("Failed to create file %q: %v", newFileName, err)
	}
	for _, item := range items.Contents {
		pieceFile, err := os.Open(*item.Key)
		if err != nil {
			log.Warnf("Failed to open piece file for reading: %v", err)
		}
		defer pieceFile.Close()

		n, err := io.Copy(newFile, pieceFile)
		if err != nil {
			log.Warnf("Failed to append piece file to big file: %v", err)
		}
		log.Infof("Wrote %d bytes of %s to the end of %s\n", n, *item.Key, newFileName)

		// Delete the old input file
		pieceFile.Close()
		if err := os.Remove(*item.Key); err != nil {
			log.Errorf("Failed to remove piece file %s: %v", *item.Key, err)
		}
	}
	newFile.Close()
	return nil
}

// DELETES A TEMPORAL FILE FROM LOCAL PATH conversations
func DeleteTemporalConversation(client string) error {
	err := os.Remove(fmt.Sprintf("%s/%s.txt", viper.GetString("s3.conversationspath"), client))

	if err != nil {
		log.Errorf("Could not delete temporal conversation file for client %s: %v", client, err)
		return err
	}
	return nil
}

// READS THE CLIENT COMBINED FILE LINE BY LINE AND APPENDS EVERY LINE CONTAINING sentence TO ANOTHER ONE AND REMOVES THE COMBINED ONE WHEN DONE
func CreateFilteredConversationFile(client string, sentence string) (string, error) {
	mainFileName := viper.GetString("s3.conversationspath") + "/" + client + ".txt"
	mainFile, err := os.Open(mainFileName)
	if err != nil {
		mainFile.Close()
		os.Remove(mainFileName)
		return "", fmt.Errorf("Failed to open main file %s for filtering: %v", mainFileName, err)
	}
	defer mainFile.Close()

	filtFileName := viper.GetString("s3.conversationspath") + "/" + client + "_filtered.txt"
	filtFile, err := os.OpenFile(filtFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	defer filtFile.Close()
	if err != nil {
		filtFile.Close()
		os.Remove(mainFileName)
		return "", fmt.Errorf("Failed to create file %q: %v", filtFileName, err)
	}

	scanner := bufio.NewScanner(mainFile)
	for scanner.Scan() {
		currentLine := scanner.Text()
		if strings.Contains(currentLine, sentence) {
			_, err = filtFile.WriteString(currentLine + "///")
			if err != nil {
				log.Warnf("Could not write new line to file: %v", err)
			}
		}
	}

	mainFile.Close()
	filtFile.Close()

	os.Remove(mainFileName)

	return filtFileName, nil
}
