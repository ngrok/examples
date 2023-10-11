package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Message struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Content  string `json:"content"`
}

var (
	clients       = make(map[int]chan Message) // Connected clients
	broadcast     = make(chan Message)         // Broadcast channel
	mutex         sync.Mutex                   // Mutex to synchronize access to clients map
	messages      []Message                    // In-memory storage for chat messages
	nextClientID  = 1                          // Next client ID
	nextMessageID = 1                          // Next message ID
)

const storageFile = "chat_messages.json"

func main() {
	http.HandleFunc("/send", handleSendMessage)
	http.HandleFunc("/receive", handleReceiveMessage)
	http.HandleFunc("/past_messages", handlePastMessages)
	http.Handle("/", http.FileServer(http.Dir("./static"))) // Static file server

	loadChatMessagesFromFile()

	go broadcastMessages()

	log.Println("Server started. Listening on port 8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var message Message
	err := decoder.Decode(&message)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	message.ID = nextMessageID
	nextMessageID++

	broadcast <- message

	saveChatMessagesToFile()
}

func handleReceiveMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET is allowed", http.StatusMethodNotAllowed)
		return
	}

	mutex.Lock()
	clientID := nextClientID
	clients[clientID] = make(chan Message, 1)
	nextClientID++
	mutex.Unlock()

	select {
	case message := <-clients[clientID]:
		json.NewEncoder(w).Encode(message)
	case <-time.After(30 * time.Second):
		w.WriteHeader(http.StatusNoContent)
	}

	mutex.Lock()
	delete(clients, clientID)
	mutex.Unlock()
}

func handlePastMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET is allowed", http.StatusMethodNotAllowed)
		return
	}

	json.NewEncoder(w).Encode(messages)
}

func broadcastMessages() {
	for message := range broadcast {
		messages = append(messages, message)

		mutex.Lock()
		for _, client := range clients {
			client <- message
		}
		mutex.Unlock()
	}
}

func saveChatMessagesToFile() {
	data, err := json.Marshal(messages)
	if err != nil {
		log.Println("Error marshaling chat messages:", err)
		return
	}

	err = os.WriteFile(storageFile, data, 0644)
	if err != nil {
		log.Println("Error writing chat messages to file:", err)
	}
}

func loadChatMessagesFromFile() {
	data, err := os.ReadFile(storageFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File does not exist yet, no need to load messages
			return
		}

		log.Println("Error reading chat messages from file:", err)
		return
	}

	err = json.Unmarshal(data, &messages)
	if err != nil {
		log.Println("Error unmarshaling chat messages:", err)
	}
}
