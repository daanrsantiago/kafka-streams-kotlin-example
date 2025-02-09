package dev.danielsantiago.kafka.streams.example.streams

import dev.danielsantiago.kafka.streams.example.config.deserializer.TransactionDeserializer
import dev.danielsantiago.kafka.streams.example.config.serializer.TransactionSerializer
import dev.danielsantiago.kafka.streams.example.model.TransactionEvent
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore

class BalancesStream(
    builder: StreamsBuilder
) {

    private val BALANCES_TOPIC = "topics.balances"
    private val transactionSerdes = Serdes.serdeFrom(TransactionSerializer(), TransactionDeserializer())
    private val eventStream = builder.stream(
        BALANCES_TOPIC,
        Consumed.with(Serdes.String(), transactionSerdes)
    )

    fun calculateBalance(clientId: String, transactionEvent: TransactionEvent, balance: Long): Long {
        if (transactionEvent.senderId == clientId) {
            return balance - transactionEvent.amountCents
        } else if ( transactionEvent.receiverId == clientId) {
            return balance + transactionEvent.amountCents
        }
        return balance
    }

    val balancesTable = eventStream
        .groupByKey()
        .aggregate(
            { 0L },
            this::calculateBalance,
            Materialized
                .`as`<String?, Long?, KeyValueStore<Bytes, ByteArray>?>("balances-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
        )

    val balancesChanges = balancesTable
        .toStream()
        .foreach { clientId, newBalance ->
            println("Saldo atualizado - Usuário: $clientId, Novo saldo: ${newBalance / 100.0} BRL")
        }
}