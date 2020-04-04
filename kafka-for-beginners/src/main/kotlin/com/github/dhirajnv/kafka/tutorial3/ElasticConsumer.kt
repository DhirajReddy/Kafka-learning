package com.github.dhirajnv.kafka.tutorial3

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.lang.NullPointerException
import java.time.Duration
import java.util.Properties
import kotlin.math.log

fun main() {
    ElasticConsumer().run()
}

open class ElasticConsumer {

    private val logger = LoggerFactory.getLogger(ElasticConsumer::class.java.name)

    fun run() {
        val restClient = createClient()

        val consumer = createKafkaConsumer("twitter_tweets")

        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))
            logger.info("received ${records.count()} messages")
            val bulkRequest = BulkRequest()
            for (record in records) {
                val jsonString = record.value()
                /* To make our indexing of tweets into elastic idempotent i.e. 
                * to make sure that the same tweet does not get into elastic twice we need to pass it an Id value.
                * Elastic index request has a field called Id. Instead of asking the elastic to generate a new Id
                * for each document we can provide that unique id by our selves.
                * First Strategy: Generate a unique Id using the "{record.topic}_{record.partition}_{record.offset}" 
                * Second Strategy: Use the unique tweet id provided by the twitter. We extract this field form
                * the tweet Json
                * */
                try {
                    val id = extractIdFromTheTwitterFeed(record.value())

                    val indexRequest = IndexRequest()
                        .index("twitter")
                        .type("tweets")
                        .id(id)
                        .source(jsonString, XContentType.JSON)

                    bulkRequest.add(indexRequest)
                } catch (e: NullPointerException) {
                    logger.warn("Skipping bad data: $jsonString")
                }
            }
            if (!records.isEmpty) {
                val bulkResponse = restClient.bulk(bulkRequest, RequestOptions.DEFAULT)
                logger.info("Bulk response received")
                bulkResponse.items.forEach {
                    logger.info("Indexed Id: ${it.id}")
                }
                logger.info("Committing offset...")
                consumer.commitSync()
                logger.info("Committed offsets")
            }
        }
        //restClient.close()
    }

    private fun extractIdFromTheTwitterFeed(twitterFeedInJson: String?): String {
        return JsonParser.parseString(twitterFeedInJson)
                        .asJsonObject
                        .get("id_str")
                        .asString
    }

    private fun createClient(): RestHighLevelClient {
        val username = ""
        val hostname = ""
        val password = ""

        val credentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(username, password))

        val restClientBuilder = RestClient.builder(HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback {
                it.setDefaultCredentialsProvider(credentialsProvider)
            }

        return RestHighLevelClient(restClientBuilder)
    }

    private fun createKafkaConsumer(topic: String): KafkaConsumer<String, String> {
        val groupId = "kafka-elastic-consumer"
        val topicOffsetResetConfig = "earliest"
        // Create Properties for consumer
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigs.BOOTSTRAP_SERVER)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, topicOffsetResetConfig)
        // To manually commit the offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        // We can use increase this config because we do a bulk index request
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")

        // Create Consumer
        val consumer = KafkaConsumer<String, String>(properties)
        consumer.subscribe(listOf(topic))
        return consumer
    }
}