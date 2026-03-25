package com.tradingassistant.operators

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.tradingassistant.model.EventType
import com.tradingassistant.model.MarketEvent
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Flattens all three Kafka source schemas into a single [MarketEvent].
 *
 * Input: raw JSON strings from price.ticks, news.raw, congress.raw
 * Output: normalised [MarketEvent] — one per input record (news may fan-out
 *         to multiple symbols if the article mentions several tickers)
 */
class EventNormaliserFunction : FlatMapFunction<String, MarketEvent> {

    private val log = LoggerFactory.getLogger(javaClass)

    @Transient
    private lateinit var mapper: ObjectMapper

    // Called once per task slot — safe place to init non-serialisable objects
    private fun getMapper(): ObjectMapper {
        if (!::mapper.isInitialized) {
            mapper = ObjectMapper().registerKotlinModule()
        }
        return mapper
    }

    override fun flatMap(raw: String, out: Collector<MarketEvent>) {
        if (raw.isBlank()) return
        try {
            val node = getMapper().readTree(raw)
            val type = node.get("type")?.asText() ?: return

            when (type) {
                "bar", "trade" -> out.collect(normalisePrice(node))
                "news"         -> normaliseNews(node).forEach(out::collect)
                "congress"     -> out.collect(normaliseCongress(node))
                else           -> log.debug("Unknown event type: $type")
            }
        } catch (e: Exception) {
            log.error("Failed to normalise event: ${raw.take(200)}", e)
        }
    }

    private fun normalisePrice(node: JsonNode) = MarketEvent(
        eventType    = EventType.PRICE,
        symbol       = node.get("symbol")?.asText() ?: return@normalisePrice null,
        timestampMs  = parseTimestampMs(node),
        open         = node.get("open")?.asDouble(),
        high         = node.get("high")?.asDouble(),
        low          = node.get("low")?.asDouble(),
        close        = node.get("close")?.asDouble() ?: node.get("price")?.asDouble(),
        volume       = node.get("volume")?.asDouble(),
        vwap         = node.get("vwap")?.asDouble(),
    ) ?: throw IllegalArgumentException("Price event missing symbol")

    private fun normaliseNews(node: JsonNode): List<MarketEvent> {
        val headline = node.get("headline")?.asText() ?: ""
        val ts = parseTimestampMs(node)

        // Pre-computed Finnhub sentiment if present
        val sentimentScore = parseFinnhubSentiment(node.get("sentiment"))

        val symbols = node.get("symbols")
            ?.map { it.asText().trim() }
            ?.filter { it.isNotBlank() && it.length <= 6 }
            ?.ifEmpty { listOf("SPY") }   // fallback: tag general news to SPY
            ?: listOf("SPY")

        // Fan-out: emit one event per related symbol so keying by symbol works
        return symbols.map { sym ->
            MarketEvent(
                eventType      = EventType.NEWS,
                symbol         = sym,
                timestampMs    = ts,
                sentimentScore = sentimentScore,
                headline       = headline.take(500),
            )
        }
    }

    private fun normaliseCongress(node: JsonNode) = MarketEvent(
        eventType       = EventType.CONGRESS,
        symbol          = node.get("ticker")?.asText() ?: "",
        timestampMs     = parseTimestampMs(node),
        transactionType = node.get("transaction_type")?.asText(),
        amountRange     = node.get("amount_range")?.asText(),
        memberName      = node.get("member_name")?.asText(),
    )

    private fun parseFinnhubSentiment(node: JsonNode?): Double? {
        node ?: return null
        return when {
            node.isObject -> {
                val bullish = node.get("bullishPercent")?.asDouble() ?: 0.0
                val bearish = node.get("bearishPercent")?.asDouble() ?: 0.0
                bullish - bearish
            }
            node.isNumber -> node.asDouble()
            else -> null
        }
    }

    private fun parseTimestampMs(node: JsonNode): Long {
        val ts = node.get("timestamp") ?: return System.currentTimeMillis()
        return if (ts.isTextual) {
            // ISO-8601 string from Alpaca
            try {
                java.time.Instant.parse(ts.asText()).toEpochMilli()
            } catch (_: Exception) {
                System.currentTimeMillis()
            }
        } else {
            // Unix seconds (Finnhub) or ms (Binance)
            val v = ts.asLong()
            if (v < 1_000_000_000_000L) v * 1000 else v
        }
    }
}
