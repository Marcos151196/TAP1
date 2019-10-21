package main

import (
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	aws "github.com/aws/aws-sdk-go/aws"
	session "github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	s3manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	sqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	viper "github.com/theherk/viper"
)

type SearchStruct struct {
	ClientSearch string
	Keysentence  string
	SearchResult string
}

type RXMsgStruct struct {
	RXMSG  *sqs.ReceiveMessageOutput
	Body   string
	SessID string
}

type ClientStruct struct {
	Client           string
	Cmd              int
	EchoConversation string
	SearchData       SearchStruct
	DownloadUser     string
	DownloadFile     string
	SessID           string
}

var clientSearch, keysentence string
var tpl *template.Template

var SearchData SearchStruct

var ReceivedEcho = make(chan RXMsgStruct, 1)
var SearchDone = make(chan RXMsgStruct, 1)

var clientName, logFile, verboseLevel, inboxURL, outboxURL string
var stdoutEnabled, fileoutEnabled bool
var cfgFile string = "config/config.toml"
var command int
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
var sess *session.Session = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))
var sqssvc *sqs.SQS = sqs.New(sess)

func main() {
	initConfig()

	go ReceiveMSGS()

	tpl = template.Must(template.ParseGlob("templates/*.gohtml"))

	http.HandleFunc("/", root)
	http.HandleFunc("/menu", menu)
	http.HandleFunc("/echo", echo)
	http.HandleFunc("/search", search)
	http.HandleFunc("/download", download)

	http.ListenAndServe(":8080", nil)
}

func root(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	ClientData := *new(ClientStruct)
	ClientData.SessID = StringWithCharset(6)

	if r.Method == http.MethodPost {
		ClientData.Client = r.FormValue("client")
		err := tpl.ExecuteTemplate(w, "menu.gohtml", ClientData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		err := tpl.ExecuteTemplate(w, "root.gohtml", ClientData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

}

func menu(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	ClientData := ClientStruct{
		Client: r.FormValue("client"),
		SessID: r.FormValue("sessid"),
	}
	if r.Method == http.MethodPost {
		ClientData.Cmd, _ = strconv.Atoi(r.FormValue("cmd"))
		if ClientData.Cmd == 1 { //ECHO
			err := tpl.ExecuteTemplate(w, "echo.gohtml", ClientData)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else if ClientData.Cmd == 2 { // SEARCH
			err := tpl.ExecuteTemplate(w, "search.gohtml", ClientData)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else if ClientData.Cmd == 3 { // DOWNLOAD
			err := tpl.ExecuteTemplate(w, "download.gohtml", ClientData)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else {
			err := tpl.ExecuteTemplate(w, "menu.gohtml", ClientData)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	} else {
		err := tpl.ExecuteTemplate(w, "menu.gohtml", ClientData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

}

func echo(w http.ResponseWriter, r *http.Request) {
	var echomsg RXMsgStruct
	cmdint, _ := strconv.Atoi(r.FormValue("cmd"))
	ClientData := ClientStruct{
		Client:           r.FormValue("client"),
		Cmd:              cmdint,
		SessID:           r.FormValue("sessid"),
		EchoConversation: r.FormValue("echoconversation"),
	}
	w.Header().Set("Content-Type", "text/html")
	if r.Method == http.MethodPost {
		timestamp := time.Now().Format("02-Jan-2006 15:04:05")

		text := r.FormValue("msgsent")
		msg := &sqs.SendMessageInput{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"clientName": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(ClientData.Client),
				},
				"sessionID": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(ClientData.SessID),
				},
				"timestamp": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(timestamp),
				},
				"cmd": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(fmt.Sprintf("%d", ClientData.Cmd)),
				},
			},
			MessageBody: aws.String(text),
			QueueUrl:    &inboxURL,
		}

		log.Infof("Sending message to AWS echo app. MESSAGE: %s", text)
		result, err := sqssvc.SendMessage(msg)
		if err != nil {
			log.Errorf("Could not send message to SQS queue: %v", err)
		} else {
			log.Infof("Message sent to SQS. MessageID: %v", *result.MessageId)
			if text == "END" {
				err := tpl.ExecuteTemplate(w, "menu.gohtml", nil)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			} else {
				for {
					echomsg = <-ReceivedEcho
					if ClientData.SessID != echomsg.SessID {
						sqssvc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
							QueueUrl:          &outboxURL,
							ReceiptHandle:     echomsg.RXMSG.Messages[0].ReceiptHandle,
							VisibilityTimeout: aws.Int64(0),
						})
						continue
					} else {
						ClientData.EchoConversation = ClientData.EchoConversation + ClientData.Client + ":\t" + text + "\nEcho:\t" + echomsg.Body + "\n\n"
						err = DeleteMSGSQS(echomsg.RXMSG)
						if err != nil {
							log.Errorf("Could not delete msg after processing: %v", err)
						}
						break
					}
				}
			}
		}

	}

	err := tpl.ExecuteTemplate(w, "echo.gohtml", ClientData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func search(w http.ResponseWriter, r *http.Request) {
	cmdint, _ := strconv.Atoi(r.FormValue("cmd"))
	ClientData := ClientStruct{
		Client: r.FormValue("client"),
		Cmd:    cmdint,
		SessID: r.FormValue("sessid"),
	}
	w.Header().Set("Content-Type", "text/html")
	if r.Method == http.MethodPost {
		ClientData.SearchData.ClientSearch = r.FormValue("clientsearch")
		ClientData.SearchData.Keysentence = r.FormValue("keysentence")

		timestamp := time.Now().Format("02-Jan-2006 15:04:05")

		msg := &sqs.SendMessageInput{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"clientName": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(ClientData.SearchData.ClientSearch),
				},
				"sessionID": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(ClientData.SessID),
				},
				"timestamp": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(timestamp),
				},
				"cmd": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(fmt.Sprintf("%d", ClientData.Cmd)),
				},
			},
			MessageBody: aws.String(ClientData.SearchData.Keysentence),
			QueueUrl:    &inboxURL,
		}

		log.Infof("Sending search command to AWS search app. KEYWORD: %s", ClientData.SearchData.Keysentence)
		result, err := sqssvc.SendMessage(msg)
		if err != nil {
			log.Errorf("Could not send message to SQS queue: %v", err)
		} else {
			log.Infof("Message sent to SQS. MessageID: %v", *result.MessageId)
			for {
				msgrx := <-SearchDone
				if ClientData.SessID != msgrx.SessID {
					sqssvc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
						QueueUrl:          &outboxURL,
						ReceiptHandle:     msgrx.RXMSG.Messages[0].ReceiptHandle,
						VisibilityTimeout: aws.Int64(0),
					})
					continue
				} else {
					ClientData.SearchData.SearchResult = msgrx.Body
					err = DeleteMSGSQS(msgrx.RXMSG)
					if err != nil {
						log.Errorf("Could not delete msg after processing: %v", err)
					}
					break
				}
			}
		}

	}

	err := tpl.ExecuteTemplate(w, "search.gohtml", ClientData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func download(w http.ResponseWriter, r *http.Request) {
	cmdint, _ := strconv.Atoi(r.FormValue("cmd"))
	ClientData := ClientStruct{
		Client: r.FormValue("client"),
		Cmd:    cmdint,
		SessID: r.FormValue("sessid"),
	}
	w.Header().Set("Content-Type", "text/html")
	if r.Method == http.MethodPost {
		ClientData.DownloadUser = r.FormValue("downloaduser")
		err := DownloadConversation(ClientData.DownloadUser)
		if err != nil {
			log.Errorf("Could not download conversation %v", err)
		}

		b, err := ioutil.ReadFile("conversations/" + ClientData.DownloadUser + ".txt")
		if err != nil {
			log.Errorf("Could not read whole conversation file to string variable: %v", err)
		}
		ClientData.DownloadFile = string(b)
		os.Remove("conversations/" + ClientData.DownloadUser + ".txt")
	}

	err := tpl.ExecuteTemplate(w, "download.gohtml", ClientData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// RECEIVING MESSAGES FROM SQS OUTBOX QUEUE THREAD
func ReceiveMSGS() {
	for {
		RXmsginput := &sqs.ReceiveMessageInput{
			MessageAttributeNames: aws.StringSlice([]string{"clientName", "sessionID", "cmd", "timestamp"}),
			QueueUrl:              &outboxURL,
			MaxNumberOfMessages:   aws.Int64(1),
			WaitTimeSeconds:       aws.Int64(1),
		}

		// READ MSG
		resultRX, err := sqssvc.ReceiveMessage(RXmsginput)
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

		fmt.Println(msgRX)
		if cRX == 1 { // ECHO
			rxmsgchan := RXMsgStruct{
				Body:   textRX,
				SessID: sessIDRX,
				RXMSG:  resultRX,
			}
			ReceivedEcho <- rxmsgchan
		} else if cRX == 2 { // SEARCH
			if textRX == "EMPTY CONVERSATION" {
				rxmsgchan := RXMsgStruct{
					Body:   textRX,
					SessID: sessIDRX,
					RXMSG:  resultRX,
				}
				log.Warnf("Could not find any lines containing that sentence for that client.")
				SearchDone <- rxmsgchan
			} else {
				rxmsgchan := RXMsgStruct{
					Body:   PrintFilteredFile(textRX),
					SessID: sessIDRX,
					RXMSG:  resultRX,
				}
				SearchDone <- rxmsgchan
			}
		}

	}
}

// DELETE MESSAGE FROM SQS QUEUE AFTER PROCESSING IT
func DeleteMSGSQS(resultRX *sqs.ReceiveMessageOutput) error {
	// DELETE MSG
	_, err := sqssvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &outboxURL,
		ReceiptHandle: resultRX.Messages[0].ReceiptHandle,
	})

	if err != nil {
		return err
	}
	return nil
}

// INIT CONFIG FILE, LOGS ETC
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

	// GET CONVERSATION PARAMETERS SPECIFIED IN CONFIG FILE
	inboxURL = viper.GetString("sqs.inboxURL")
	outboxURL = viper.GetString("sqs.outboxURL")

	return
}

// GENERATE RANDOM SESSION ID
func StringWithCharset(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// DOWNLOAD CONVERSATION DIRECTLY FROM S3 GIVEN THE USERNAME
func DownloadConversation(client string) error {
	s3svc := s3.New(sess)
	bucketname := viper.GetString("s3.bucketname")

	// List all conversations ([S3_CONVERSATIONS_PATH]/[USERNAME]_[SESSION_ID].txt) that begin with [S3_CONVERSATIONS_PATH]/[USERNAME]
	resp, err := s3svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucketname), Prefix: aws.String(viper.GetString("s3.conversationspath") + "/" + client)})
	if err != nil {
		return fmt.Errorf("Unable to list items in bucket %q, %v", bucketname, err)
	}

	// Download every session from our user and when finished, combine all those files in a local one called [S3_CONVERSATIONS_PATH]/[USERNAME].txt
	downloader := s3manager.NewDownloader(sess)
	// Loop for downloading every session from our user and store them in local folder [S3_CONVERSATIONS_PATH]
	for _, item := range resp.Contents {
		downloadPath := *item.Key
		// Local session file
		f, err := os.OpenFile(downloadPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("Failed to create file %q, %v", downloadPath, err)
		}

		// Write the contents of S3 session file to the local session file
		_, err = downloader.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(bucketname),
			Key:    aws.String(downloadPath),
		})
		f.Close()
		if err != nil {
			return fmt.Errorf("failed to download file, %v", err)
		}
	}

	// Combine all sessions in [S3_CONVERSATIONS_PATH]/[USERNAME].txt
	CombineSessionsToFile(client, resp)
	log.Infof("Whole conversation of client %s has been downloaded.", client)
	return nil
}

// COMBINE MULTIPLE SESSION FILES ([S3_CONVERSATIONS_PATH]/[USERNAME]_[SESSION_ID].txt) IN ONE ([S3_CONVERSATIONS_PATH]/[USERNAME].txt)
func CombineSessionsToFile(client string, items *s3.ListObjectsV2Output) error {
	newFileName := "conversations/" + client + ".txt"
	os.Remove(newFileName)
	newFile, err := os.OpenFile(newFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	defer newFile.Close()
	if err != nil {
		return fmt.Errorf("Failed to create file %q: %v", newFileName, err)
	}
	// Iterate over session files and append them to the combined one
	for _, item := range items.Contents {
		// Open session file
		pieceFile, err := os.Open(*item.Key)
		if err != nil {
			log.Warnf("Failed to open piece file for reading: %v", err)
		}
		defer pieceFile.Close()

		// Append session file
		n, err := io.Copy(newFile, pieceFile)
		if err != nil {
			log.Warnf("Failed to append piece file to big file: %v", err)
		}
		log.Infof("Wrote %d bytes of %s to the end of %s\n", n, *item.Key, newFileName)

		// Delete session file
		pieceFile.Close()
		if err := os.Remove(*item.Key); err != nil {
			log.Errorf("Failed to remove piece file %s: %v", *item.Key, err)
		}
	}
	newFile.Close()
	return nil
}

// PRINT FILTERED FILE ON CONSOLE
func PrintFilteredFile(file string) string {
	var fileString = ""
	// fmt.Println("\nFiltered conversation:")
	lines := strings.Split(file, "///")
	for _, line := range lines {
		if line != "" {
			columns := strings.Split(line, "|||")
			fileString = fmt.Sprintf("%s%s    %s\n", fileString, columns[0], columns[1])
		}
	}
	fileString = fmt.Sprintf("%s\n", fileString)
	return fileString
}
