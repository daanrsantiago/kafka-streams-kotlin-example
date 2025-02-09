package dev.danielsantiago.kafka.streams.example.model

import java.util.Date

data class ClientCreatedEvent (
    val id: String,
    val name: String,
    val birthDate: Date,
) {
}