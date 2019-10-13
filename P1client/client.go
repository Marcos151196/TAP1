package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	sqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	viper "github.com/theherk/viper"
)

var clientName, logFile, verboseLevel, inboxURL, outboxURL string
var stdoutEnabled, fileoutEnabled bool
var cfgFile string = "config/config.toml"
var command int
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var sessID string
var sess *session.Session = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))
var svc *sqs.SQS = sqs.New(sess)
var msgSent = make(chan bool, 1)
var msgPrinted = make(chan bool, 1)

func main() {
	initConfig()

	for {
		// MAIN MENU
		reader := bufio.NewReader(os.Stdin)
		log.Infof("Client name: %s\n\n", clientName)
		fmt.Printf("1-ECHO\n2-SEARCH\n3-DOWNLOAD\nSelect the command(number + ENTER): ")
		for {
			c, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Could not read command: %v. Try again: ", err)
				continue
			}

			c = strings.TrimSuffix(c, "\n")
			command, err = strconv.Atoi(c)
			if err != nil || (command != 1 && command != 2 && command != 3) {
				fmt.Printf("Invalid command, try again: ")
				continue
			} else {
				fmt.Printf("Command %d selected.\n", command)
				break
			}
		}

		// RECEIVE MSGS
		go func() {
			for {
				RXmsginput := &sqs.ReceiveMessageInput{
					MessageAttributeNames: aws.StringSlice([]string{"clientName", "sessionID", "cmd"}),
					QueueUrl:              &outboxURL,
					MaxNumberOfMessages:   aws.Int64(1),
					WaitTimeSeconds:       aws.Int64(5),
				}

				// READ MSG
				resultRX, err := svc.ReceiveMessage(RXmsginput)
				if err != nil {
					log.Errorf("Error while receiving message: %v", err)
					continue
				}

				if len(resultRX.Messages) == 0 {
					continue
				}
				msgRX := *resultRX.Messages[0]
				textRX := *msgRX.Body
				sessIDRX := *msgRX.MessageAttributes["sessionID"].StringValue
				cmdRX := *msgRX.MessageAttributes["cmd"].StringValue
				cRX, _ := strconv.Atoi(cmdRX)
				if sessID != sessIDRX {
					svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
						QueueUrl:          &outboxURL,
						ReceiptHandle:     msgRX.ReceiptHandle,
						VisibilityTimeout: aws.Int64(0),
					})
					continue
				}

				// ECHO
				if cRX == 1 {
					log.Infof("Echoed message: %s", textRX)
				}
				err = DeleteMSGSQS(resultRX)
				if err != nil {
					log.Errorf("Could not delete msg after processing: %v", err)
				}

			}
		}()

		// MESSAGE SENDING LOOP
		for {
			if command == 1 {
				fmt.Printf("Write the message you want to echo: ")
				text, err := reader.ReadString('\n')
				if err != nil {
					log.Errorf("Could not read string: %v", err)
				}
				text = strings.TrimSuffix(text, "\n")

				sessID = StringWithCharset(6)
				timestamp := time.Now().Format("02-Jan-2006 15:04:05")

				msg := &sqs.SendMessageInput{
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
							StringValue: aws.String(fmt.Sprintf("%d", command)),
						},
					},
					MessageBody: aws.String(text),
					QueueUrl:    &inboxURL,
				}

				log.Infof("Sending message to AWS: %s", text)
				result, err := svc.SendMessage(msg)
				if err != nil {
					log.Errorf("Could not send message to SQS queue: %v", err)
					continue
				} else {
					log.Infof("Message sent to SQS. MessageID: %v", *result.MessageId)
					if text == "END" {
						break
					}
				}
			} else if command == 2 {
				fmt.Printf("Write the name of the client: ")
				text, err := reader.ReadString('\n')
				if err != nil {
					log.Errorf("Could not read string: %v", err)
				}
				text = strings.TrimSuffix(text, "\n")

				sessID = StringWithCharset(6)
			} else if command == 3 {
				fmt.Printf("Write the name of the client: ")
				client, err := reader.ReadString('\n')
				if err != nil {
					log.Errorf("Could not read string: %v", err)
					break
				}
				client = strings.TrimSuffix(client, "\n")
				err = DownloadConversation(client)
				if err != nil {
					log.Errorf("Could not download conversation %v", err)
				}
				break
			}
		}

	}

}

func DeleteMSGSQS(resultRX *sqs.ReceiveMessageOutput) error {
	// DELETE MSG
	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &outboxURL,
		ReceiptHandle: resultRX.Messages[0].ReceiptHandle,
	})

	if err != nil {
		return err
	}
	return nil
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
	clientName = viper.GetString("general.clientName")
	logFile = fmt.Sprintf("%s/%s.log", viper.GetString("log.logfilepath"), clientName)
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

func StringWithCharset(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func DownloadConversation(client string) error {

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)
	filename := fmt.Sprintf("downloadedConv/%s.txt", client)
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
	log.Infof("File %s.txt downloaded, %d bytes.", client, n)
	return nil
}
