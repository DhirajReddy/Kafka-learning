package org.github.com.dhirajreddy13

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KeyValueMapper
import org.apache.kafka.streams.kstream.ValueJoiner
import java.util.Properties

fun main() {
    UserDataStreamsApp().run()
}

open class UserDataStreamsApp {
    fun run() {
        val properties = getProperties()
        val builder = StreamsBuilder()

        val userRecordTable = builder.globalTable<String, String>("user-record")
        val purchaseStream = builder.stream<String, String>("purchase-record")

        // inner join
        purchaseStream.join(userRecordTable,
            KeyValueMapper { key, value -> key },
            ValueJoiner { purchaseRecord, userRecord -> "Purchase = $purchaseRecord, UserInfo = $userRecord"}
        ).to("user-data-inner-join")

        // left join
        purchaseStream.leftJoin(userRecordTable,
            KeyValueMapper { key, value -> key },
            ValueJoiner { purchaseRecord, userRecord -> {
                if (userRecord.isNullOrEmpty()) {
                    "Purchase = $purchaseRecord, UserInfo = null"
                }
                else {
                    "Purchase = $purchaseRecord, UserInfo = $userRecord"
                }
            }}
        ).to("user-data-left-join")

        val streams = KafkaStreams(builder.build(), properties)
        streams.cleanUp()
        streams.start()

        Runtime.getRuntime().addShutdownHook( Thread(streams::close))
    }

    private fun getProperties(): Properties {
        val properties = Properties()
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "user-data-streams-app")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        return properties
    }
}