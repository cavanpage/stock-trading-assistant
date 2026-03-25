package com.tradingassistant

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.tradingassistant.model.FeatureVector
import com.tradingassistant.model.MarketEvent
import com.tradingassistant.model.SignalOutput
import com.tradingassistant.operators.AsyncInferenceFunction
import com.tradingassistant.operators.EventNormaliserFunction
import com.tradingassistant.operators.FeatureWindowFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.util.concurrent.TimeUnit

/**
 * Entry point for the Flink signal processor job.
 *
 * Topology:
 *
 *   Kafka(price.ticks)  ─┐
 *   Kafka(news.raw)     ─┼──► EventNormaliser ──► FeatureWindow (keyed by symbol)
 *   Kafka(congress.raw) ─┘         │
 *                                  └──► AsyncInference ──► Kafka(signals.generated)
 *
 * Submit to a running Flink cluster:
 *   flink run -c com.tradingassistant.SignalProcessorJobKt \
 *     target/flink-signal-processor-0.1.0.jar
 *
 * Run locally (mini-cluster, dev only):
 *   mvn exec:java -Dexec.mainClass=com.tradingassistant.SignalProcessorJobKt
 */

private val mapper = ObjectMapper().registerKotlinModule()

fun main() {
    val kafka     = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
    val inference = System.getenv("INFERENCE_SERVER_URL")    ?: "http://localhost:8000/internal/inference"

    val env = StreamExecutionEnvironment.getExecutionEnvironment().apply {
        parallelism = 4
        // Enable checkpointing every 30s — state survives restarts
        enableCheckpointing(30_000)
    }

    // ── Sources ────────────────────────────────────────────────────────────

    fun kafkaSource(topic: String) = KafkaSource.builder<String>()
        .setBootstrapServers(kafka)
        .setTopics(topic)
        .setGroupId("flink-signal-processor")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()

    val priceStream   = env.fromSource(kafkaSource("price.ticks"),   WatermarkStrategy.noWatermarks(), "price-ticks")
    val newsStream    = env.fromSource(kafkaSource("news.raw"),      WatermarkStrategy.noWatermarks(), "news-raw")
    val congressStream= env.fromSource(kafkaSource("congress.raw"),  WatermarkStrategy.noWatermarks(), "congress-raw")

    // ── Normalise and union ────────────────────────────────────────────────

    val events = priceStream
        .union(newsStream, congressStream)
        .flatMap(EventNormaliserFunction())
        .name("event-normaliser")
        .filter { it.symbol.isNotBlank() }

    // ── Key by symbol → stateful feature windows ───────────────────────────

    val featureVectors = events
        .keyBy(MarketEvent::symbol)
        .process(FeatureWindowFunction())
        .name("feature-window")

    // ── Async inference — non-blocking HTTP to Python model server ─────────
    // unorderedWait: don't wait for in-order results — throughput > ordering here
    // capacity=100: at most 100 in-flight requests per task slot before backpressure

    val signals = AsyncDataStream.unorderedWait(
        featureVectors,
        AsyncInferenceFunction(inferenceUrl = inference, timeoutMs = 2_000),
        2_000,
        TimeUnit.MILLISECONDS,
        /* capacity = */ 100,
    ).name("async-inference")

    // ── Sink: signals.generated ───────────────────────────────────────────

    val sink = KafkaSink.builder<String>()
        .setBootstrapServers(kafka)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder<String>()
                .setTopic("signals.generated")
                .setValueSerializationSchema(SimpleStringSchema())
                .build()
        )
        .build()

    signals
        .map { mapper.writeValueAsString(it) }
        .name("serialise-signal")
        .sinkTo(sink)
        .name("signals-kafka-sink")

    env.execute("trading-signal-processor")
}
