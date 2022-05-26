package io.github.clemenscode.eventliquefier.kafka

import io.github.clemenscode.eventliquefier.model.EventToLiquefy
import io.github.clemenscode.eventliquefier.utils.getLogger
import org.springframework.kafka.core.KafkaTemplate

class KafkaProducer(
        private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = getLogger(KafkaProducer::class.java)

    fun trySendEvent(eventToLiquefy: EventToLiquefy): Boolean {
        return try {
            kafkaTemplate.send(eventToLiquefy.topic, eventToLiquefy.payload)
            true
        } catch (e: Throwable) {
            logger.error("An error occurred while sending the event: {} wit the target topic: {} busIdentifier: {}", eventToLiquefy.payload, eventToLiquefy.topic, eventToLiquefy.busIdentifier)
            false
        }
    }
}
