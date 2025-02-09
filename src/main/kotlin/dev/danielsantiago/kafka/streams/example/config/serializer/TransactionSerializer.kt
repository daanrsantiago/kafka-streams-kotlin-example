package dev.danielsantiago.kafka.streams.example.config.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.danielsantiago.kafka.streams.example.model.TransactionEvent
import org.apache.kafka.common.serialization.Serializer

class TransactionSerializer: Serializer<TransactionEvent> {

    private val objectMapper = ObjectMapper()
        .registerKotlinModule()

    override fun serialize(topic: String?, data: TransactionEvent?): ByteArray? {
        try {
            if (data == null) return null
            return objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            println(e.message)
            println("Error while serializing transaction")
            return null
        }
    }
}