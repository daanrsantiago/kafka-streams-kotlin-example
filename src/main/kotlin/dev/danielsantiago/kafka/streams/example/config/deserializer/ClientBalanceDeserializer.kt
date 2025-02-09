package dev.danielsantiago.kafka.streams.example.config.deserializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import dev.danielsantiago.kafka.streams.example.model.ClientBalance
import org.apache.kafka.common.serialization.Deserializer

class ClientBalanceDeserializer: Deserializer<ClientBalance> {

    private val objectMapper = ObjectMapper()
        .registerKotlinModule()

    override fun deserialize(topic: String?, data: ByteArray?): ClientBalance {
        return  objectMapper.readValue(data, ClientBalance::class.java)
    }
}