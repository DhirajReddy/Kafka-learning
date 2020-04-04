package com.github.dhirajnv.kafka.tutorial2

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.Hosts
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


fun main() {
    val twitterProducer = TwitterProducer()
    twitterProducer.run()
}

open class TwitterProducer {
    val log = LoggerFactory.getLogger(TwitterProducer::class.java.name)
    private val accessToken = ""
    private val accessTokenSecret = ""
    private val consumerKey = ""
    private val consumerSecret = ""

    // Optional: set up some followings and track terms
    private val terms: List<String> = Lists.newArrayList("kafka")

    fun run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        val msgQueue: BlockingQueue<String?> = LinkedBlockingQueue(1000)

        // Create Twitter client
        val client = createTwitterClient(msgQueue)

        // Attempts to establish connection
        client.connect()

        // Create kafka producer
        val kafkaProducer = createKafkaProducer()
        val topic = "twitter_tweets"

        // on a different thread, or multiple different threads....
        while (!client.isDone) {
            val msg = msgQueue.poll(5, TimeUnit.SECONDS)
            msg?.let {
                log.info(it)
                val producerRecord = ProducerRecord<String, String>(topic, null, it)
                kafkaProducer.send(producerRecord) { metadata, e ->
                    e?.let {
                        log.error("Something BAD happened", e)
                    }
                }
            }
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            log.info("Shutting Down App initiated..")
            log.info("Shutting Down client")
            client.stop()
            log.info("Shutting Down kafka producer")
            // We do this so that producer sends all the messages in its memory in to the cluster
            kafkaProducer.close()
            log.info("<<<<< End of Application >>>>>")
        })

        log.info("<<<<< End of Application >>>>>")
    }

    private fun createTwitterClient(msgQueue: BlockingQueue<String?>): Client {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)  */
        val hosebirdHosts: Hosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint()

        hosebirdEndpoint.trackTerms(terms)

        val hosebirdAuth: Authentication = OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret)

        val builder = ClientBuilder()
            .name("Hosebird-Client-01") // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(StringDelimitedProcessor(msgQueue))

        return builder.build()
    }

    private fun createKafkaProducer(): KafkaProducer<String, String> {
        // Set Producer properties
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfigs.BOOTSTRAP_SERVER)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        // Create a safe producer
        // This will take care of setting other properties, but just to be explicit we will also set them
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE.toString())
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")

        // High through put solution with expense of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString())
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")

        // Initialise and return producer
        return KafkaProducer<String, String>(properties)
    }
}
/* Notes:
1. in-flight requests:
This property is configured on the producer side by max.in.flight.requests.per.connection property.
The connection here is understood as the broker and thus,
this property represents the number of not processed requests that can be buffered on the producer's side.
It was introduced to minimize the waiting time of the clients by letting them send requests
even though some of the prior ones are still processed by the broker.
Max number messages that are intended to a partition from a producer that can be buffered before the partition broker
processes the message.

The number of messages that can be sent over wire without receiving the ack.
If the number of such messages exceeds the setting, the new messages are not sent, they will be in the queue.
*/