package dev.danielsantiago.kafka.streams.example.config.deserializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValues
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.danielsantiago.kafka.streams.example.model.DepositEvent
import org.apache.kafka.common.serialization.Deserializer

class DepositDeserializer: Deserializer<DepositEvent> {

    private val objectMapper = ObjectMapper()
        .registerKotlinModule()

    override fun deserialize(topic: String?, data: ByteArray?): DepositEvent {
        return objectMapper.readValue(data, DepositEvent::class.java)
    }
}