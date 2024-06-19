package models

type SendRequest struct {
	FromId  int    `json:"fromId"`
	ToId    int    `json:"toId"`
	Message string `json:"message"`
}

type User struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type Notification struct {
	From    User   `json:"from"`
	To      User   `json:"to"`
	Message string `json:"message"`
}
