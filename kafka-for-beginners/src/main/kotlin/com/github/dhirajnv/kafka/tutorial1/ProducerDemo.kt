package com.github.dhirajnv.kafka.tutorial1

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

fun main() {
    // Set Producer properties
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigs.BOOTSTRAP_SERVER)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

    // Initialise producer
    val producer = KafkaProducer<String, String>(properties)

    // Create Producer record : Here you specify the name of the topic
    val producerRecord = ProducerRecord<String, String>(
        "first_topic",
        "Hello world from Kotlin"
    )

    // Send producer record : Async
    producer.send(producerRecord)

    // Flush data -> Needed to tell the producer class to push the data to the socket
    producer.flush()

    // Flush data and close producer
    producer.close()
}