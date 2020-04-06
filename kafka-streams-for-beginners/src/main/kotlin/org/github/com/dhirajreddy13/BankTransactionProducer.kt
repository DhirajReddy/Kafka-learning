package org.github.com.dhirajreddy13

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Utils.sleep
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.Properties
import kotlin.random.Random

fun main() {
    BankTransactionProducer().run()
}

open class BankTransactionProducer {
    private val logger = LoggerFactory.getLogger("BankTransactionProducer")
    fun run() {
        val properties = getProperties()
        val producer = KafkaProducer<String, String>(properties)
        var i = 1
        try {
            while (true) {
                logger.info("Sending batch $i")
                producer.send(getNewProducerRecord("alice"))
                sleep(100)
                producer.send(getNewProducerRecord("bob"))
                sleep(100)
                producer.send(getNewProducerRecord("mark"))
                sleep(100)
                i++
            }
        } catch (e : Exception) {
            logger.error("Something bad happened", e)
        }
        producer.close()
    }

    private fun getNewProducerRecord(name: String): ProducerRecord<String, String>? {
        val transaction = JsonNodeFactory.instance.objectNode()
        transaction.put("name", name)
        val amount = Random.nextInt(0, 100)
        transaction.put("amount", amount)
        val time = Instant.now()
        transaction.put("time", time.toEpochMilli())
        return ProducerRecord("bank-transactions", name, transaction.toString())
    }

    private fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
        return properties
    }

}