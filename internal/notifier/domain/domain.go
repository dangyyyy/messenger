package domain

type UserRegistered struct {
	EventID string
	UserID  int64
	Email   string
}
type Notification struct {
	EventID string
	UserID  int64
	Email   string
	Kind    string
}

const KindWelcome = "welcome"
