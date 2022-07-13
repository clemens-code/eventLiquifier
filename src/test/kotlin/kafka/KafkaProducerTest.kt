package io.github.clemenscode.eventliquefier.kafka

import io.github.clemenscode.eventliquefier.model.EventToLiquefy
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate

internal class KafkaProducerTest {

    private val kafkaTemplate = mockk<KafkaTemplate<String, String>>(relaxed = true)

    private lateinit var kafkaProducer: KafkaProducer

    @BeforeEach
    fun init() {
        clearAllMocks()
        kafkaProducer = KafkaProducer(kafkaTemplate)
    }

    @Test
    fun testTrySendEvent() {
        val event = EventToLiquefy("topic", "id", "payload")
        val result = kafkaProducer.trySendEvent(event)
        verify(exactly = 1) { kafkaTemplate.send(any(), any()) }
        assertThat(result).isTrue
    }

    @Test
    fun testTrySendEventError() {
        val event = EventToLiquefy("topic", "id", "payload")
        every { kafkaTemplate.send(any(), any()) }.throws(Exception())
        val result = kafkaProducer.trySendEvent(event)
        verify(exactly = 1) { kafkaTemplate.send(any(), any()) }
        assertThat(result).isFalse
    }
}