package io.github.clemenscode.eventliquefier.rabbitmq

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DeliverCallback
import io.github.clemenscode.eventliquefier.utils.getLogger
import kotlinx.coroutines.runBlocking
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import java.io.IOException

private const val PREFETCH_COUNT = 5

class ChannelProvider(
        private val connectionFactory: CachingConnectionFactory,
        private val deliveryCallback: DeliverCallback
) {

    private lateinit var channel: Channel

    private val logger = getLogger(ChannelProvider::class.java)

    init {
        createChannel()
    }

    private fun createChannel(): Channel{
        return connectionFactory.rabbitConnectionFactory.newConnection().run {
            createChannel()
        }.apply {
            val cancelCallback = CancelCallback {
                runBlocking {
                    closeChannel(this@apply)
                }
            }
            basicQos(PREFETCH_COUNT)
            basicConsume(QUEUE_NAME, deliveryCallback, cancelCallback)
        }
    }

    fun tryAck(deliveryTag: Long) {
        return try {
            channel.basicAck(deliveryTag, false)
        }catch (e: IOException){
            logger.error("An Error occurred while acknowledging a message!", e)
        }
    }

    fun tryNack(deliveryTag: Long) {
        return try {
            channel.basicNack(deliveryTag, false, true)
        }catch (e: IOException){
            logger.error("An Error occurred while rejecting a message!", e)
        }
    }

    private fun closeChannel(channel: Channel) {
        try {
            channel.close()
        }catch (e: IOException){
            logger.error("An Error occurred while closing the Channel!", e)
        }
    }

    fun cancel() {
        closeChannel(channel)
    }
}
