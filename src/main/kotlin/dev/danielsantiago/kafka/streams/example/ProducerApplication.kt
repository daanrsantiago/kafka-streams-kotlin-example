package dev.danielsantiago.kafka.streams.example

import dev.danielsantiago.kafka.streams.example.model.TransactionEvent
import dev.danielsantiago.kafka.streams.example.producer.TransactionProducer
import kotlin.random.Random

fun main() {

    val transactionProducer = TransactionProducer()

    while (true) {
        Thread.sleep(200)
        val amount = Random.nextLong(100L,2000L)
        val transaction = if (Math.random() > 0.5) {
            TransactionEvent (
                senderId = "Daniel",
                receiverId = "Iago",
                amountCents = amount
            )
        } else {
            TransactionEvent (
                senderId = "Iago",
                receiverId = "Daniel",
                amountCents = amount
            )
        }
        transactionProducer.publishTransaction(transaction)
    }

}