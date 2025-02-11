package dev.danielsantiago.kafka.streams.example.streams

import dev.danielsantiago.kafka.streams.example.config.deserializer.BalanceChangeDeserializer
import dev.danielsantiago.kafka.streams.example.config.serializer.BalanceChangeSerializer
import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent
import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent.BalanceChangeEventType.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import kotlin.collections.aggregate

class BalancesTable {

    companion object {

        private val BALANCES_TOPIC = "topics.balances"
        private val balanceChangeSerdes = Serdes.serdeFrom(BalanceChangeSerializer(), BalanceChangeDeserializer())

        fun buildTable(builder: StreamsBuilder) {
            val eventStream = builder.stream(
                BALANCES_TOPIC,
                Consumed.with(Serdes.String(), balanceChangeSerdes)
            ).peek { key, value ->
                println("User ${value.clientId} ${value.type} ${value.amountCents / 100.0} BRL")
            }

            eventStream
                .groupByKey()
                .aggregate(
                    { 0L },
                    this::calculateBalance,
                    Materialized
                        .`as`<String?, Long?, KeyValueStore<Bytes, ByteArray>?>("balances-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                )
        }

        fun calculateBalance(clientId: String, balanceChangeEvent: BalanceChangeEvent, balance: Long): Long {
            val newBalance = when (balanceChangeEvent.type) {
                WITHDRAW -> balance - balanceChangeEvent.amountCents
                DEPOSIT -> balance + balanceChangeEvent.amountCents
                SEND -> balance - balanceChangeEvent.amountCents
                RECEIVE -> balance + balanceChangeEvent.amountCents
            }
            println("Updated Balance - User: $clientId, New Balance: ${newBalance / 100.0} BRL")
            return newBalance
        }
    }
}