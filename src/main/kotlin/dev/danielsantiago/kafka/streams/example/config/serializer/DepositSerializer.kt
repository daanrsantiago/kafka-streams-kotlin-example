package dev.danielsantiago.kafka.streams.example.config.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.danielsantiago.kafka.streams.example.model.DepositEvent
import org.apache.kafka.common.serialization.Serializer

class DepositSerializer: Serializer<DepositEvent> {

    private val objectMapper = ObjectMapper()
        .registerKotlinModule()

    override fun serialize(topic: String?, data: DepositEvent?): ByteArray {
        return objectMapper.writeValueAsBytes(data)
    }
}