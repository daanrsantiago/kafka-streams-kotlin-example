package dev.danielsantiago.kafka.streams.example.model

class DepositEvent (
    val clientId: String,
    val amountCents: Long
)