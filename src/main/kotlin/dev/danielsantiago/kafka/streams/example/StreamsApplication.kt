package dev.danielsantiago.kafka.streams.example

import dev.danielsantiago.kafka.streams.example.streams.BalancesStream
import dev.danielsantiago.kafka.streams.example.streams.TransactionStream
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.*
import java.util.*

fun main() {

    val settings = Properties()
    settings[APPLICATION_ID_CONFIG] = "balance-application"
    settings[BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    settings[COMMIT_INTERVAL_MS_CONFIG] = "100"
    settings[DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    settings[DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.Long().javaClass

    val builder = StreamsBuilder()
    val balancesStream = BalancesStream(builder)
    val transactionStream = TransactionStream(builder)

    val kafkaStreams = KafkaStreams(builder.build(), settings)
    kafkaStreams.start()

    Runtime.getRuntime().addShutdownHook(Thread { kafkaStreams.close() })
}