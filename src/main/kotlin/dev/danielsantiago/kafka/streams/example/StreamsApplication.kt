package dev.danielsantiago.kafka.streams.example

import dev.danielsantiago.kafka.streams.example.streams.BalancesTable
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.*
import java.util.*

fun main() {

    val settings = Properties()
    settings[APPLICATION_ID_CONFIG] = "balance-application"
    settings[BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    settings[COMMIT_INTERVAL_MS_CONFIG] = "100"
    settings[PROCESSING_GUARANTEE_CONFIG]= "exactly_once_v2"

    val builder = StreamsBuilder()
    val balancesStream = BalancesTable.buildTable(builder)

    val kafkaStreams = KafkaStreams(builder.build(), settings)
    kafkaStreams.start()

    Runtime.getRuntime().addShutdownHook(Thread { kafkaStreams.close() })
}