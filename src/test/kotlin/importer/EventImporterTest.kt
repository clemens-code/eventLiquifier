package io.github.clemenscode.eventliquefier.importer

import io.github.clemenscode.eventliquefier.kafka.KafkaProducer
import io.github.clemenscode.eventliquefier.model.EventToLiquefy
import io.github.clemenscode.eventliquefier.model.PendingEventToLiquefy
import io.github.clemenscode.eventliquefier.rabbitmq.RabbitMqConsumer
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@ExperimentalCoroutinesApi
@ObsoleteCoroutinesApi
internal class EventImporterTest {
    private val dispatcher = StandardTestDispatcher()
    private val rabbitMqConsumer = mockk<RabbitMqConsumer>(relaxed = true)
    private val producer = mockk<KafkaProducer>(relaxed = true)

    private lateinit var eventImporter: EventImporter

    @BeforeEach
    fun setUp() {
        clearAllMocks()
        eventImporter = EventImporter(rabbitMqConsumer, producer)
    }

    @Test
    fun testSendMessageSuccess() {
        val message = PendingEventToLiquefy(EventToLiquefy("test", "test", "test"), 1L)
        coEvery {
            rabbitMqConsumer.collectNextMessages(any(), any())
        }.returns(listOf(message))

        coEvery { producer.trySendEvent(any()) }.returns(true)
        runTest(dispatcher) {
            eventImporter.import()
        }
        coVerify { rabbitMqConsumer.ackMessage(message) }
    }

    @Test
    fun testSendMessageFailed() {
        val message = PendingEventToLiquefy(EventToLiquefy("test", "test", "test"), 1L)
        coEvery {
            rabbitMqConsumer.collectNextMessages(any(), any())
        }.returns(listOf(message))

        coEvery { producer.trySendEvent(any()) }.returns(false)
        runTest(dispatcher) {
            eventImporter.import()
        }
        coVerify { rabbitMqConsumer.nackMessage(message) }
    }

}