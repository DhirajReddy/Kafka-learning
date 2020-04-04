package com.github.dhirajnv.kafka.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

/* Run this consumer multiple times after "enabling" allow run in parallel in edit configuration.
* Every time a consumer is added to the consumer group, the group re balances.
* The act of re balancing will distribute the partitions in the topic to different consumers*/
fun main() {
    val LOGGER = LoggerFactory.getLogger("ConsumerDemo")
    // Giving a unique value for the group id is important for the "earliest" setting.
    // If there is a consumer group with already the same id, then as earlier messages
    // might have been in that group, kafka will not give them to this consumer
    // If unique consumer group id is presented, earliest will retrieve all the messages from the beginning
    val groupId = "my-kotlin-consumer-app1"
    val topic = "first_topic"
    val topicOffsetResetConfig = "earliest"
    // Create Properties for consumer
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigs.BOOTSTRAP_SERVER)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, topicOffsetResetConfig)

    // Create Consumer
    val consumer = KafkaConsumer<String, String>(properties)

    // Subscribe to topic
    consumer.subscribe(listOf(topic))

    // Poll consumer to get data
    while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        for (record in records) {
            LOGGER.info("Key: ${record.key()}, Value: ${record.value()} \n" +
                    "Partition: ${record.partition()}, topic: ${record.topic()} \n" +
                    "Offset: ${record.offset()}")
        }
    }
}