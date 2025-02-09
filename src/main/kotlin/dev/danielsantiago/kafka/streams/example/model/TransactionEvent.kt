package dev.danielsantiago.kafka.streams.example.model

data class TransactionEvent(
    val senderId: String,
    val receiverId: String,
    val amountCents: Long
) {
}