package dev.danielsantiago.kafka.streams.example.model

data class TransferenceEvent(
    val senderId: String,
    val receiverId: String,
    val amountCents: Long
) {
}