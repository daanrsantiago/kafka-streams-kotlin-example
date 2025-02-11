package dev.danielsantiago.kafka.streams.example.config.deserializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.danielsantiago.kafka.streams.example.model.BalanceChangeEvent
import org.apache.kafka.common.serialization.Deserializer

class BalanceChangeDeserializer: Deserializer<BalanceChangeEvent> {

    private val objectMapper = ObjectMapper()
        .registerKotlinModule()

    override fun deserialize(topic: String?, data: ByteArray?): BalanceChangeEvent {
        return objectMapper.readValue(data, BalanceChangeEvent::class.java)
    }
}