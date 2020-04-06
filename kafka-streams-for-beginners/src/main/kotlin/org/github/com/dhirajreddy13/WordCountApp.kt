package org.github.com.dhirajreddy13

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import java.util.Properties

fun main() {
    WordCountApp().run()
}
open class WordCountApp {

    private fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app")
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        return properties
    }

    fun run() {
        val properties = getProperties()

        val streamBuilder = StreamsBuilder()

        val wordCountInputStream = streamBuilder
            .stream<String, String>("word-count-input")

        // (null, "Kafka kafka streams")
        val kTable = wordCountInputStream
                // (null, "kafka kafka streams")
                .mapValues { value -> value.toLowerCase() }
                // (null, "kafka"), (null, "kafka"), (null, "streams")
                .flatMapValues { value -> value.split(" ") }
                // ("kafka", "kafka"), ("kafka", "kafka"), ("streams", "streams")
                .selectKey { _, value ->  value }
                // [("kafka", "kafka"), ("kafka", "kafka")], [("streams", "streams")]
                .groupByKey()
                // ("kafka", 2), ("streams", 1)
                .count(Materialized.`as`("count-store"))

        kTable.toStream().to( "word-count-output", Produced.with(Serdes.String(), Serdes.Long()))

        val kafkaStreams = KafkaStreams(streamBuilder.build(), properties)
        kafkaStreams.start()

        // Prints topology
        print(kafkaStreams.toString())

        Runtime.getRuntime().addShutdownHook(Thread {
            kafkaStreams.close()
        })
    }
}