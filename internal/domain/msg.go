package domain

type Msg struct {
	Partition int32  `json:"partition"`
	Key       string `json:"key"`
	Topic     string `json:"topic"`
	Content   string `json:"content"`
}
