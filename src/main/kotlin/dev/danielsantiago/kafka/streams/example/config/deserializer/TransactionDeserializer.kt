package dev.danielsantiago.kafka.streams.example.config.deserializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.danielsantiago.kafka.streams.example.model.TransactionEvent
import org.apache.kafka.common.serialization.Deserializer

class TransactionDeserializer: Deserializer<TransactionEvent> {

    private val objectMapper = ObjectMapper()
        .registerKotlinModule()

    override fun deserialize(topic: String?, data: ByteArray?): TransactionEvent? {
        try {
            if (data == null) return null
            return objectMapper.readValue(data, TransactionEvent::class.java)
        } catch (e: Exception) {
            println(e.message)
            e.printStackTrace()
            println("Error while serializing transaction")
            return null
        }
    }
}