package dev.danielsantiago.kafka.streams.example

import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent
import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent.BalanceChangeEventType.DEPOSIT
import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent.BalanceChangeEventType.WITHDRAW
import dev.danielsantiago.kafka.streams.example.model.TransferenceEvent
import dev.danielsantiago.kafka.streams.example.producer.BalanceChangeProducer
import dev.danielsantiago.kafka.streams.example.producer.TransferenceProducer
import kotlin.random.Random

fun main() {

    val balanceChangeProducer = BalanceChangeProducer()
    val transferenceProducer = TransferenceProducer()

    Thread.startVirtualThread {
        while (true) {
            Thread.sleep(10000)
            val amount = 100L * Random.nextLong(1L,20L)
            val transference = if (Math.random() > 0.5) {
                TransferenceEvent (
                    senderId = "Daniel",
                    receiverId = "Iago",
                    amountCents = amount
                )
            } else {
                TransferenceEvent (
                    senderId = "Iago",
                    receiverId = "Daniel",
                    amountCents = amount
                )
            }
            transferenceProducer.publishTransference(transference)
        }
    }

    while (true) {
        Thread.sleep(10000)
        val balanceChange = BalanceChangeEvent (
            clientId = if (Math.random() > 0.5) "Daniel" else "Iago",
            type = if (Math.random() > 0.5) DEPOSIT else WITHDRAW,
            amountCents = 100L * Random.nextLong(1L,20L)
        )
        balanceChangeProducer.publishBalanceChange(balanceChange)
    }

}