package io.github.clemenscode.eventliquefier.rabbitmq

import com.rabbitmq.client.DeliverCallback
import io.github.clemenscode.eventliquefier.model.PendingEventToLiquefy
import io.github.clemenscode.eventliquefier.utils.getLogger
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import java.time.Duration
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy
import org.springframework.beans.factory.annotation.Value

class RabbitMqConsumer(
        private val connectionFactory: CachingConnectionFactory
) {

    @Value("\${rabbitmq.v-host")
    private lateinit var virtualHost: String

    private val logger = getLogger(RabbitMqConsumer::class.java)

    private lateinit var channelProvider: ChannelProvider

    private val eventChannel = EventChannel().getEventChannel()

    init {
        connectionFactory.virtualHost = virtualHost
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
            channelProvider = ChannelProvider(connectionFactory, callback)
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
