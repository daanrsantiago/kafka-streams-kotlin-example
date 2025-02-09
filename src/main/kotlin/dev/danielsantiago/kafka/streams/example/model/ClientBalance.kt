package dev.danielsantiago.kafka.streams.example.model

data class ClientBalance(
    val clientId: String,
    val balance: Long = 0L
)
