package io.github.clemenscode.eventliquefier.importer

import io.github.clemenscode.eventliquefier.kafka.KafkaProducer
import io.github.clemenscode.eventliquefier.model.PendingEventToLiquefy
import io.github.clemenscode.eventliquefier.rabbitmq.RabbitMqConsumer

private const val NEXT_MESSAGES = 500

class EventImporter(
        private val rabbitMqConsumer: RabbitMqConsumer,
        private val producer: KafkaProducer
) : Importer() {


    override suspend fun import() {
        processEvents(rabbitMqConsumer.collectNextMessages(limit = NEXT_MESSAGES))
    }

    private fun processEvents(messages: List<PendingEventToLiquefy>) {
        messages.forEach {
            if (producer.trySendEvent(it.eventToLiquefy)) {
                rabbitMqConsumer.ackMessage(it)
            } else {
                rabbitMqConsumer.nackMessage(it)
            }
        }
    }
}
