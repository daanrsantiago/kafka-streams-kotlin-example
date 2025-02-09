package dev.danielsantiago.kafka.streams.example.model

data class WithdrawEvent(
    val clientId: String,
    val amountCents: Long
)