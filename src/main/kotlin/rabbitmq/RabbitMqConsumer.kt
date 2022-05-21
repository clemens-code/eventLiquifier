package io.github.clemenscode.eventliquefier.rabbitmq

import com.rabbitmq.client.DeliverCallback
import io.github.clemenscode.eventliquefier.model.PendingEventToLiquefy
import io.github.clemenscode.eventliquefier.utils.getLogger
import kotlinx.coroutines.*
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import java.time.Duration
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

private const val VIRTUAL_HOST = "/eventLiquefier"
internal const val QUEUE_NAME = "eventsPending"

class RabbitMqConsumer(
        private val connectionFactory: CachingConnectionFactory,
        private val dispatcher: CoroutineDispatcher,
        private val channelSize: Long
) {
    private val logger = getLogger(RabbitMqConsumer::class.java)

    private lateinit var channelProvider: ChannelProvider

    private val eventChannel = EventChannel().getEventChannel()

    init {
        connectionFactory.virtualHost = VIRTUAL_HOST
    }

    @PostConstruct
    fun setUp() {
        val callback = DeliverCallback {_, message ->
            runBlocking {
                val extracted = extractEventToLiquefy(message, logger)
                if (extracted != null) {
                    eventChannel.send(extracted)
                }else {
                    channelProvider.tryAck(message.envelope.deliveryTag)
                }
            }
        }
        runBlocking {
            channelProvider = ChannelProvider(connectionFactory, dispatcher, callback)
        }
    }

    fun ackMessage(message: PendingEventToLiquefy) {
        channelProvider.tryAck(message.deliveryTag)
    }

    fun nackMessage(message: PendingEventToLiquefy){
        channelProvider.tryNack(message.deliveryTag)
    }

    suspend fun collectSingeMessage() = eventChannel.receive()

    suspend fun collectNextMessages(timeoutSec: Long = 1L, limit: Int = 100): List<PendingEventToLiquefy>{
        val buffer = mutableListOf<PendingEventToLiquefy>()
        withTimeoutOrNull(Duration.ofSeconds(timeoutSec).toMillis()){
            while (buffer.size < limit) {
                buffer.add(eventChannel.receive())
            }
        }
        return buffer
    }

    @PreDestroy
    fun tearDown() {
        channelProvider.cancel()
    }
}