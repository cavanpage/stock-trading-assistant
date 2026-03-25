package com.tradingassistant.operators

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.tradingassistant.model.FeatureVector
import com.tradingassistant.model.SignalOutput
import com.tradingassistant.model.SignalType
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.TimeUnit

private val JSON_MEDIA_TYPE = "application/json; charset=utf-8".toMediaType()

/**
 * Async inference call to the Python model server.
 *
 * Uses [AsyncDataStream.unorderedWait] so the Flink task never blocks
 * waiting for a response — other events continue processing concurrently.
 * OkHttp's async callback wires the response back to Flink's [ResultFuture].
 *
 * Timeout: 2s hard limit — if the model server is slow we drop the call
 * and emit a HOLD signal rather than building up backpressure.
 */
class AsyncInferenceFunction(
    private val inferenceUrl: String,
    private val timeoutMs: Long = 2_000L,
) : RichAsyncFunction<FeatureVector, SignalOutput>() {

    private val log = LoggerFactory.getLogger(javaClass)

    @Transient private lateinit var client: OkHttpClient
    @Transient private lateinit var mapper: ObjectMapper

    override fun open(cfg: Configuration) {
        client = OkHttpClient.Builder()
            .callTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .connectionPool(ConnectionPool(10, 5, TimeUnit.MINUTES))
            .build()
        mapper = ObjectMapper().registerKotlinModule()
    }

    override fun close() {
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }

    override fun asyncInvoke(fv: FeatureVector, resultFuture: ResultFuture<SignalOutput>) {
        val body = mapper.writeValueAsString(fv.toRequestMap())
        val request = Request.Builder()
            .url(inferenceUrl)
            .post(body.toRequestBody(JSON_MEDIA_TYPE))
            .build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                log.warn("Inference call failed for ${fv.symbol}: ${e.message}")
                resultFuture.complete(listOf(holdSignal(fv)))
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    if (!it.isSuccessful) {
                        log.warn("Inference returned ${it.code} for ${fv.symbol}")
                        resultFuture.complete(listOf(holdSignal(fv)))
                        return
                    }
                    try {
                        val json = mapper.readTree(it.body!!.string())
                        val signal = SignalOutput(
                            symbol          = fv.symbol,
                            signalType      = SignalType.valueOf(json.get("signal_type").asText()),
                            confidence      = json.get("confidence").asDouble(),
                            compositeScore  = json.get("composite_score").asDouble(),
                            quantScore      = json.get("quant_score").asDouble(),
                            sentimentScore  = json.get("sentiment_score").asDouble(),
                            congressModifier= json.get("congress_modifier").asDouble(),
                            triggerEvent    = fv.triggerEvent.name,
                            timestampMs     = fv.timestampMs,
                            featureSnapshot = mapOf(
                                "rsi14"       to fv.rsi14,
                                "macd"        to fv.macd,
                                "bbPosition"  to fv.bbPosition,
                                "ema9Cross"   to fv.ema9Cross,
                                "volRatio"    to fv.volRatio,
                                "avgSentiment"to fv.avgSentiment,
                            ),
                        )
                        resultFuture.complete(listOf(signal))
                    } catch (ex: Exception) {
                        log.error("Failed to parse inference response for ${fv.symbol}", ex)
                        resultFuture.complete(listOf(holdSignal(fv)))
                    }
                }
            }
        })
    }

    /** Emit a neutral HOLD signal so the stream stays unblocked on failure. */
    private fun holdSignal(fv: FeatureVector) = SignalOutput(
        symbol          = fv.symbol,
        signalType      = SignalType.HOLD,
        confidence      = 0.0,
        compositeScore  = 0.0,
        quantScore      = 0.0,
        sentimentScore  = fv.avgSentiment,
        congressModifier= fv.congressModifier,
        triggerEvent    = fv.triggerEvent.name,
        timestampMs     = fv.timestampMs,
        featureSnapshot = emptyMap(),
    )
}

/** Convert [FeatureVector] to the flat dict the Python inference endpoint expects. */
private fun FeatureVector.toRequestMap(): Map<String, Any?> = mapOf(
    "symbol"           to symbol,
    "timestamp"        to timestampMs.toString(),
    "trigger_event"    to triggerEvent.name.lowercase(),
    "bar_count"        to barCount,
    "avg_sentiment"    to avgSentiment,
    "congress_modifier"to congressModifier,
    "quant_features"   to mapOf(
        "close"          to close,
        "ema9"           to ema9,
        "ema21"          to ema21,
        "ema50"          to ema50,
        "ema9_cross"     to ema9Cross,
        "price_vs_ema50" to priceVsEma50,
        "rsi14"          to rsi14,
        "rsi_oversold"   to rsiOversold,
        "rsi_overbought" to rsiOverbought,
        "macd"           to macd,
        "bb_position"    to bbPosition,
        "atr"            to atr,
        "vol_ratio"      to volRatio,
    ),
)
