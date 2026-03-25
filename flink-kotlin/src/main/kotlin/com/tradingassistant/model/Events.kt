package com.tradingassistant.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Unified internal event — output of EventNormaliserFunction.
 * All three Kafka sources (price, news, congress) are mapped to this.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class MarketEvent(
    val eventType: EventType,
    val symbol: String,
    val timestampMs: Long,

    // Price fields (null for non-price events)
    val open: Double? = null,
    val high: Double? = null,
    val low: Double? = null,
    val close: Double? = null,
    val volume: Double? = null,
    val vwap: Double? = null,

    // Sentiment fields (null for non-news events)
    val sentimentScore: Double? = null,   // [-1, 1], null = needs scoring
    val headline: String? = null,

    // Congressional fields (null for non-congress events)
    val transactionType: String? = null,  // "Purchase" | "Sale"
    val amountRange: String? = null,
    val memberName: String? = null,
)

enum class EventType { PRICE, NEWS, CONGRESS }

/**
 * Feature vector emitted by FeatureWindowFunction.
 * Consumed by AsyncInferenceFunction.
 */
data class FeatureVector(
    val symbol: String,
    val timestampMs: Long,
    val triggerEvent: EventType,
    val barCount: Int,

    // Technical indicators (computed over the price window)
    val close: Double,
    val ema9: Double,
    val ema21: Double,
    val ema50: Double,
    val ema9Cross: Double,         // 1.0 if ema9 > ema21, else 0.0
    val priceVsEma50: Double,      // (close - ema50) / ema50
    val rsi14: Double,
    val rsiOversold: Double,       // 1.0 if RSI < 30
    val rsiOverbought: Double,     // 1.0 if RSI > 70
    val macd: Double,
    val bbPosition: Double,        // 0=at lower band, 1=at upper band
    val atr: Double,
    val volRatio: Double,          // current volume / 20-bar avg

    // Sentiment (averaged over the news window)
    val avgSentiment: Double,
    val sentimentSampleCount: Int,

    // Congressional modifier [-1, 1]
    val congressModifier: Double,
)

/**
 * Signal emitted to the signals.generated Kafka topic.
 */
data class SignalOutput(
    val symbol: String,
    val signalType: SignalType,
    val confidence: Double,        // [0, 1]
    val compositeScore: Double,    // [-1, 1]
    val quantScore: Double,
    val sentimentScore: Double,
    val congressModifier: Double,
    val triggerEvent: String,
    val timestampMs: Long,
    // Snapshot of key features for explainability / dashboard display
    val featureSnapshot: Map<String, Double>,
)

enum class SignalType { BUY, SELL, HOLD }
