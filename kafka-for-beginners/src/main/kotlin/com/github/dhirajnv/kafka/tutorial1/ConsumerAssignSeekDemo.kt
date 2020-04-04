package com.github.dhirajnv.kafka.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

/* Here we need not use the group id as assign and seek is only used to check the data
* for debugging purposes or analysis purposes */
fun main() {
    val LOGGER = LoggerFactory.getLogger("ConsumerDemo")
    // Giving a unique value for the group id is important for the "earliest" setting.
    // If there is a consumer group with already the same id, then as earlier messages
    // might have been in that group, kafka will not give them to this consumer
    // If unique consumer group id is presented, earliest will retrieve all the messages from the beginning
    // ~val groupId = "my-kotlin-consumer-app1"~
    val topic = "first_topic"
    val topicOffsetResetConfig = "earliest"
    // Create Properties for consumer
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigs.BOOTSTRAP_SERVER)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    // properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, topicOffsetResetConfig)

    // Create Consumer
    val consumer = KafkaConsumer<String, String>(properties)

    // assign
    // I want to check only the messages in first_topic at partition 1
    val topicPartition = TopicPartition(topic, 0)
    consumer.assign(listOf(topicPartition))

    // seek
    // here i tell the consumer from which offset position to seek the values
    consumer.seek(topicPartition, 10)


    // I want to check only 5 values in topic first_topic at partition 1 and offset 10
    val numOfMessagesToRead = 5
    var keepReading = true
    var numOfRecordsRead = 0

    // Poll consumer to get data
    while (keepReading) {
        val records = consumer.poll(Duration.ofMillis(100))
        records.map { record ->
           {
               numOfRecordsRead++
               if (numOfMessagesToRead > numOfMessagesToRead) {
                   keepReading = false
               }
               else {
                   LOGGER.info(
                       "Key: ${record.key()}, Value: ${record.value()} \n" +
                               "Partition: ${record.partition()}, topic: ${record.topic()} \n" +
                               "Offset: ${record.offset()}"
                   )
               }
           }
        }
    }
}