package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gabrielmh13/notifications-service/pkg/models"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

var ErrUserNotFoundInProducer = errors.New("user not found")

func findUserById(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.Id == id {
			return user, nil
		}
	}

	return models.User{}, ErrUserNotFoundInProducer
}

func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, message string, fromId, toId int) error {
	fromUser, err := findUserById(fromId, users)
	if err != nil {
		return err
	}

	toUser, err := findUserById(toId, users)
	if err != nil {
		return err
	}

	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: message,
	}

	notificartionJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.Id)),
		Value: sarama.StringEncoder(notificartionJSON),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}

func sendMessageHandler(producer sarama.SyncProducer, users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context) {

		var req models.SendRequest
		if err := ctx.BindJSON(&req); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}

		err := sendKafkaMessage(producer, users, req.Message, req.FromId, req.ToId)
		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
			return
		}
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			return
		}

		ctx.JSON(http.StatusOK, gin.H{"message": "Message sent successfully"})
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer %v", err)
	}

	return producer, nil
}

func main() {
	users := []models.User{
		{Id: 1, Name: "John"},
		{Id: 2, Name: "Jane"},
		{Id: 3, Name: "Doe"},
		{Id: 4, Name: "Smith"},
	}

	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("Failed to initialize producer: %v", err)
	}
	defer producer.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	router.POST("/send", sendMessageHandler(producer, users))

	fmt.Printf("Producer is running on port %s\n", ProducerPort)

	if err := router.Run(ProducerPort); err != nil {
		log.Printf("Failed to start producer: %v", err)
	}
}
