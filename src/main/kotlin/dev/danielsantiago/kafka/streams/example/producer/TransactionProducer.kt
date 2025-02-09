package dev.danielsantiago.kafka.streams.example.producer

import dev.danielsantiago.kafka.streams.example.model.TransactionEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

class TransactionProducer {

    private val TRANSACTION_TOPIC = "topics.transactions"
    private val properties = Properties()
    private val producer: KafkaProducer<String, TransactionEvent>

    init {
        properties["bootstrap.servers"] = "localhost:9092"
        properties["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        properties["value.serializer"] = "dev.danielsantiago.kafka.streams.example.config.serializer.TransactionSerializer"

        producer = KafkaProducer(properties)

        Runtime.getRuntime().addShutdownHook(Thread { producer.close() })
    }

    fun publishTransaction(transaction: TransactionEvent) {
        println("Publishing transaction")
        val record = ProducerRecord(TRANSACTION_TOPIC, transaction.senderId, transaction)
        producer.send(record) { metadata, ex ->
            println("Sent transaction from ${transaction.senderId} to ${transaction.receiverId} of ${transaction.amountCents} cents for the partition ${metadata.partition()}")
        }
    }

}