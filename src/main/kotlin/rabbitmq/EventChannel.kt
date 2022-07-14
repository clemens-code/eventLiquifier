package io.github.clemenscode.eventliquefier.rabbitmq

import io.github.clemenscode.eventliquefier.model.PendingEventToLiquefy
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking

private const val CHANNEL_CAPACITY = 10_000

internal class EventChannel {

    fun getEventChannel() =
            Channel<PendingEventToLiquefy>(CHANNEL_CAPACITY, onUndeliveredElement = { resendMessage(it) })

    private fun resendMessage(message: PendingEventToLiquefy) {
        runBlocking {
            getEventChannel().send(message)
        }
    }
}
