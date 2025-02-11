package dev.danielsantiago.kafka.streams.example.producer

import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent
import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent.BalanceChangeEventType.RECEIVE
import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent.BalanceChangeEventType.SEND
import dev.danielsantiago.kafka.streams.example.model.TransferenceEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class BalanceChangeProducer {

    private val BALANCE_CHANGE_TOPIC = "topics.balances"
    private val properties = Properties()
    private val producer: KafkaProducer<String, BalanceChangeEvent>

    init {
        properties[BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        properties[KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        properties[VALUE_SERIALIZER_CLASS_CONFIG] = "dev.danielsantiago.kafka.streams.example.config.serializer.BalanceChangeSerializer"

        producer = KafkaProducer(properties)

        Runtime.getRuntime().addShutdownHook(Thread { producer.close() })
    }

    fun publishBalanceChange(balanceChangeEvent: BalanceChangeEvent) {
        val record = ProducerRecord(BALANCE_CHANGE_TOPIC, balanceChangeEvent.clientId, balanceChangeEvent)
        producer.send(record) { metadata, ex ->
            println("Sent balance change of type ${balanceChangeEvent.type} to client ${balanceChangeEvent.clientId} with amount ${balanceChangeEvent.amountCents} cents for the partition ${metadata.partition()}")
        }
    }

    fun publishTransference(transference: TransferenceEvent) {
        producer.beginTransaction()
        val senderBalanceChangeEvent = BalanceChangeEvent(
            type = SEND,
            clientId = transference.senderId,
            senderId = transference.senderId,
            receiverId = transference.receiverId,
            amountCents = transference.amountCents
        )
        val receiverBalanceChangeEvent = BalanceChangeEvent(
            type = RECEIVE,
            clientId = transference.receiverId,
            senderId = transference.senderId,
            receiverId = transference.receiverId,
            amountCents = transference.amountCents
        )
        publishBalanceChange(senderBalanceChangeEvent)
        publishBalanceChange(receiverBalanceChangeEvent)
        producer.commitTransaction()
    }
}