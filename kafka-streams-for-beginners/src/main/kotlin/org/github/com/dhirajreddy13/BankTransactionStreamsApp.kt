package org.github.com.dhirajreddy13

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.Properties


fun main() {
    BankTransactionStreamsApp().run()
}

open class BankTransactionStreamsApp {
    private val logger = LoggerFactory.getLogger("BankTransactionStreamsApp")
    fun run() {
        val properties = getProperties()

        val serializer = JsonSerializer()
        val deserializer = JsonDeserializer()
        val jsonSerde = Serdes.serdeFrom(serializer, deserializer)

        val builder = StreamsBuilder()

        // create the initial json object for balances
        val initialBalance = JsonNodeFactory.instance.objectNode()
        initialBalance.put("count", 0)
        initialBalance.put("balance", 0)
        initialBalance.put("time", Instant.ofEpochMilli(0L).toEpochMilli())

        builder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonSerde))
            .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
            .aggregate(
                { initialBalance },
                {_, transaction, balance -> newBalance(transaction, balance)},
                Named.`as`("bank-agg-store"),
                Materialized.with(Serdes.String(), jsonSerde))
            .toStream()
            .to("bank-balance-exactly-once", Produced.with(Serdes.String(), jsonSerde))

        val kafkaStream = KafkaStreams(builder.build(), properties)

        kafkaStream.cleanUp()
        kafkaStream.start()

        logger.info("Topology $kafkaStream")

        Runtime.getRuntime().addShutdownHook(Thread {
            kafkaStream.cleanUp()
            kafkaStream.close()
        })
    }

    private fun newBalance(transaction: JsonNode, balance: JsonNode): JsonNode {
        val newBalance = JsonNodeFactory.instance.objectNode()

        newBalance.put("count", balance.get("count").asInt() + 1)
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt())
        val maxTime = maxOf(balance.get("time").asLong(), transaction.get("time").asLong())
        newBalance.put("time", Instant.ofEpochMilli(maxTime).toEpochMilli())
        return newBalance as JsonNode
    }

    private fun getProperties(): Properties {
        val properties = Properties()

        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app")

        return properties
    }
}