package io.github.clemenscode.eventliquefier.model

data class EventToLiquefy(
        val topic: String,
        val busIdentifier: String,
        val payload: String
)

data class PendingEventToLiquefy(
        val eventToLiquefy: EventToLiquefy,
        val deliveryTag: Long
)
