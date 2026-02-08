package main

// Job structs from examples
type ProcessWebhook struct {
	WebhookID  string            `json:"webhook_id"`
	Endpoint   string            `json:"endpoint"`
	Payload    map[string]any    `json:"payload"`
	Headers    map[string]string `json:"headers"`
	RetryCount int               `json:"retry_count"`
}

type ProcessOrder struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type SendEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type ProcessPayment struct {
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

type ProcessRefund struct {
	RefundID string  `json:"refund_id"`
	Amount   float64 `json:"amount"`
	Reason   string  `json:"reason"`
}

type SendNotification struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

// JobName methods
func (p ProcessWebhook) JobName() string {
	return "process_webhook"
}

func (p ProcessOrder) JobName() string {
	return "process_order"
}

func (s SendEmail) JobName() string {
	return "send_email"
}

func (p ProcessPayment) JobName() string {
	return "process_payment"
}

func (p ProcessRefund) JobName() string {
	return "process_refund"
}

func (s SendNotification) JobName() string {
	return "send_notification"
}
