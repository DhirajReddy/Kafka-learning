package org.github.com.dhirajreddy13

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory
import java.util.Properties

fun main() {
    FavoriteColor().run()
}

open class FavoriteColor {

    private val log = LoggerFactory.getLogger("FavoriteColor")

    private fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-app")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        return properties
    }

    fun run() {
        val properties = getProperties()

        val streamBuilder = StreamsBuilder()
        val userInput = streamBuilder.table<String, String>("user-fav-color")

        val allowedColors = listOf("red","blue","green")

        userInput.toStream()
            .mapValues { value -> value.toLowerCase() }
            .filter { _, value -> allowedColors.contains(value) }
            .selectKey { _, value -> value}
            .to("fav-color-tmp")

        /* A group by on stream only increases the count, if the key has no entries in the stream the entry is not
         displayed.
         A group by on KTable increases and decreases count values on the keys.
         It publishes to the topic all the keys even with the keys with zero value unlike streams
         Be it KGroupedStream or KGroupedTable, count always increases or decreases based on the key.

         Both null keys and null values are ignored during the count operation in KGroupedStreams.
         Only null keys are ignored in KGroupedTable where as null values caused the key to be deleted.
        */
        streamBuilder.table<String, String>("fav-color-tmp")
            .groupBy { _, value -> KeyValue(value, value) }
            .count(Named.`as`("colorCountStore"))
            .toStream()
            .to("color-count", Produced.with(Serdes.String(), Serdes.Long()))

        val appStream = KafkaStreams(streamBuilder.build(), properties)

        log.info("Before Start")
        log.info(appStream.toString())

        appStream.start()

        log.info("After Start")
        log.info(appStream.toString())

        Runtime.getRuntime().addShutdownHook(Thread {
            appStream.close()
        })
    }
}