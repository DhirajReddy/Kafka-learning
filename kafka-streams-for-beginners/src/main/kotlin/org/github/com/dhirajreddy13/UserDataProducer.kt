package org.github.com.dhirajreddy13

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Utils.sleep
import org.slf4j.LoggerFactory
import java.util.Properties

fun main() {
    UserDataProducer().run()
}

open class UserDataProducer {

    private val logger = LoggerFactory.getLogger(UserDataProducer::class.java.name)

    fun run() {
        val properties = getProperties()

        val kafkaProducer = KafkaProducer<String, String>(properties)

        // 1 - we create a new user, then we send some data to Kafka
        logger.info("\nExample 1 - new user\n")
        kafkaProducer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get()
        kafkaProducer.send(purchaseRecord("john", "Apples and Bananas (1)")).get()

        sleep(10000)

        // 2 - we receive user purchase, but it doesn't exist in Kafka
        logger.info("\nExample 2 - non existing user\n")
        kafkaProducer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get()

        sleep(10000)

        // 3 - we update user "john", and send a new transaction
        logger.info("\nExample 3 - update to user\n")
        kafkaProducer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get()
        kafkaProducer.send(purchaseRecord("john", "Oranges (3)")).get()

        sleep(10000)

        // 4 - we send a user purchase for stephane, but it exists in Kafka later
        logger.info("\nExample 4 - non existing user then user\n")
        kafkaProducer.send(purchaseRecord("stephane", "Computer (4)")).get()
        kafkaProducer.send(userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).get()
        kafkaProducer.send(purchaseRecord("stephane", "Books (4)")).get()
        kafkaProducer.send(userRecord("stephane", null)).get() // delete for cleanup

        sleep(10000)

        // 5 - we create a user, but it gets deleted before any purchase comes through
        logger.info("\nExample 5 - user then delete then data\n")
        kafkaProducer.send(userRecord("alice", "First=Alice")).get()
        kafkaProducer.send(userRecord("alice", null)).get() // that's the delete record
        kafkaProducer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get()

        sleep(10000)
    }

    private fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
        return properties
    }

    private fun userRecord(key: String, value: String?): ProducerRecord<String, String?> {
        return ProducerRecord("user-record", key, value)
    }

    private fun purchaseRecord(key: String, value: String?): ProducerRecord<String, String?> {
        return ProducerRecord("purchase-record", key, value)
    }
}