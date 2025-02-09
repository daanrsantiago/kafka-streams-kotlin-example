package dev.danielsantiago.kafka.streams.example.streams

import dev.danielsantiago.kafka.streams.example.config.deserializer.TransactionDeserializer
import dev.danielsantiago.kafka.streams.example.config.serializer.TransactionSerializer
import dev.danielsantiago.kafka.streams.example.model.TransactionEvent
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced

class TransactionStream (
    builder: StreamsBuilder
) {

    private val TRANSACTION_TOPIC = "topics.transactions"
    private val transactionSerdes = Serdes.serdeFrom(TransactionSerializer(), TransactionDeserializer())
    private val eventStream = builder.stream(
        TRANSACTION_TOPIC,
        Consumed.with(Serdes.String(), transactionSerdes)
    )

    val sendSenderTransactionsToWithdraw = eventStream
        .map { key: String, value: TransactionEvent ->
            KeyValue(value.senderId, value)
        }.to("topics.balances", Produced.with(Serdes.String(), transactionSerdes))

    val sendReceiverTransactionsToDeposit = eventStream
        .map { key: String, value ->
            KeyValue(value.receiverId, value)
        }.to("topics.balances", Produced.with(Serdes.String(), transactionSerdes))

}