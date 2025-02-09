package dev.danielsantiago.kafka.streams.example.config.deserializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.danielsantiago.kafka.streams.example.model.WithdrawEvent
import org.apache.kafka.common.serialization.Deserializer

class WithdrawDeserializer: Deserializer<WithdrawEvent> {

    private val objectMapper = ObjectMapper()
        .registerKotlinModule()

    override fun deserialize(topic: String?, data: ByteArray?): WithdrawEvent {
        return objectMapper.readValue(data, WithdrawEvent::class.java)
    }
}