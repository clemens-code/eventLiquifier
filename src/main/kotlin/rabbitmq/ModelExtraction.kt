package io.github.clemenscode.eventliquefier.rabbitmq

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.Delivery
import io.github.clemenscode.eventliquefier.model.EventToLiquefy
import io.github.clemenscode.eventliquefier.model.PendingEventToLiquefy
import org.slf4j.Logger

private val objectMapper = ObjectMapper().apply {
    registerModule(JavaTimeModule())
    registerKotlinModule()
}

internal fun extractEventToLiquefy(message: Delivery, logger: Logger): PendingEventToLiquefy? =
        try {
            objectMapper.readValue(message.body, PendingEventToLiquefy::class.java)
        }catch (e: Throwable){
            logger.error("Could not read value from message!", e)
            null
        }