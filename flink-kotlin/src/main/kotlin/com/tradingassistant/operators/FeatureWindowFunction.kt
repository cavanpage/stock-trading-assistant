package com.tradingassistant.operators

import com.tradingassistant.model.EventType
import com.tradingassistant.model.FeatureVector
import com.tradingassistant.model.MarketEvent
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.io.Serializable

private const val PRICE_WINDOW = 60   // bars — 1hr at 1-min resolution
private const val NEWS_WINDOW  = 20   // sentiment scores
private const val MIN_BARS     = 26   // need at least 26 bars for EMA26/MACD

/**
 * Stateful operator — one instance per symbol key, backed by RocksDB in prod.
 *
 * Maintains:
 *   - Rolling deque of last [PRICE_WINDOW] OHLCV bars
 *   - Rolling list of last [NEWS_WINDOW] sentiment scores
 *   - Latest congressional modifier
 *
 * On every incoming event it updates the relevant state then emits a
 * [FeatureVector] if there's enough price history to compute indicators.
 * Running at 1-min bar resolution this is ~1 emit/min per symbol.
 */
class FeatureWindowFunction
    : KeyedProcessFunction<String, MarketEvent, FeatureVector>() {

    private val log = LoggerFactory.getLogger(javaClass)

    // ── Flink managed state ────────────────────────────────────────────────
    // Stored as serialised Bar objects in ListState so RocksDB handles persistence.

    private lateinit var priceState: ListState<Bar>
    private lateinit var sentimentState: ListState<Double>
    private lateinit var congressState: ValueState<Double>

    data class Bar(
        val open: Double, val high: Double, val low: Double,
        val close: Double, val volume: Double,
    ) : Serializable

    override fun open(cfg: Configuration) {
        priceState = runtimeContext.getListState(
            ListStateDescriptor("price-bars", Bar::class.java)
        )
        sentimentState = runtimeContext.getListState(
            ListStateDescriptor("sentiment-scores", Types.DOUBLE)
        )
        congressState = runtimeContext.getState(
            ValueStateDescriptor("congress-modifier", Types.DOUBLE)
        )
    }

    override fun processElement(
        event: MarketEvent,
        ctx: KeyedProcessFunction<String, MarketEvent, FeatureVector>.Context,
        out: Collector<FeatureVector>,
    ) {
        when (event.eventType) {
            EventType.PRICE     -> updatePrice(event)
            EventType.NEWS      -> updateSentiment(event)
            EventType.CONGRESS  -> updateCongress(event)
        }

        val bars = priceState.get().toList()
        if (bars.size < MIN_BARS) return   // not enough history yet

        val features = computeFeatures(bars)
        val sentimentScores = sentimentState.get().toList()
        val avgSentiment = if (sentimentScores.isEmpty()) 0.0
                           else sentimentScores.average()
        val congressMod = congressState.value() ?: 0.0

        out.collect(
            FeatureVector(
                symbol             = event.symbol,
                timestampMs        = event.timestampMs,
                triggerEvent       = event.eventType,
                barCount           = bars.size,
                close              = features.close,
                ema9               = features.ema9,
                ema21              = features.ema21,
                ema50              = features.ema50,
                ema9Cross          = features.ema9Cross,
                priceVsEma50       = features.priceVsEma50,
                rsi14              = features.rsi14,
                rsiOversold        = features.rsiOversold,
                rsiOverbought      = features.rsiOverbought,
                macd               = features.macd,
                bbPosition         = features.bbPosition,
                atr                = features.atr,
                volRatio           = features.volRatio,
                avgSentiment       = avgSentiment,
                sentimentSampleCount = sentimentScores.size,
                congressModifier   = congressMod,
            )
        )
    }

    // ── State update helpers ───────────────────────────────────────────────

    private fun updatePrice(event: MarketEvent) {
        val close = event.close ?: return
        val bar = Bar(
            open   = event.open   ?: close,
            high   = event.high   ?: close,
            low    = event.low    ?: close,
            close  = close,
            volume = event.volume ?: 0.0,
        )
        val bars = priceState.get().toMutableList()
        bars.add(bar)
        if (bars.size > PRICE_WINDOW) {
            priceState.update(bars.takeLast(PRICE_WINDOW))
        } else {
            priceState.add(bar)
        }
    }

    private fun updateSentiment(event: MarketEvent) {
        val score = event.sentimentScore ?: return
        val scores = sentimentState.get().toMutableList()
        scores.add(score)
        sentimentState.update(
            if (scores.size > NEWS_WINDOW) scores.takeLast(NEWS_WINDOW) else scores
        )
    }

    private fun updateCongress(event: MarketEvent) {
        val modifier = congressModifier(event.transactionType, event.amountRange)
        congressState.update(modifier)
        log.info("Congress modifier updated for ${event.symbol}: $modifier (${event.memberName})")
    }

    // ── Feature computation ────────────────────────────────────────────────

    private data class Indicators(
        val close: Double, val ema9: Double, val ema21: Double, val ema50: Double,
        val ema9Cross: Double, val priceVsEma50: Double,
        val rsi14: Double, val rsiOversold: Double, val rsiOverbought: Double,
        val macd: Double, val bbPosition: Double, val atr: Double, val volRatio: Double,
    )

    private fun computeFeatures(bars: List<Bar>): Indicators {
        val closes  = bars.map { it.close }
        val highs   = bars.map { it.high }
        val lows    = bars.map { it.low }
        val volumes = bars.map { it.volume }

        val close = closes.last()
        val ema9  = ema(closes.takeLast(9),  9)
        val ema21 = ema(closes.takeLast(21), 21)
        val ema50 = ema(closes.takeLast(minOf(50, closes.size)), minOf(50, closes.size))
        val ema12 = ema(closes.takeLast(12), 12)
        val ema26 = ema(closes.takeLast(26), 26)
        val macd  = ema12 - ema26
        val rsi   = rsi(closes.takeLast(15))

        val window20 = closes.takeLast(20)
        val bbMid    = window20.average()
        val bbStd    = window20.map { (it - bbMid).let { d -> d * d } }.average().let { Math.sqrt(it) }
        val bbUpper  = bbMid + 2 * bbStd
        val bbLower  = bbMid - 2 * bbStd
        val bbPos    = if (bbUpper != bbLower) (close - bbLower) / (bbUpper - bbLower) else 0.5

        val atr = atr(highs.takeLast(15), lows.takeLast(15), closes.takeLast(15))

        val avgVol   = volumes.takeLast(20).average().coerceAtLeast(1.0)
        val volRatio = volumes.last() / avgVol

        return Indicators(
            close        = close,
            ema9         = ema9,
            ema21        = ema21,
            ema50        = ema50,
            ema9Cross    = if (ema9 > ema21) 1.0 else 0.0,
            priceVsEma50 = (close - ema50) / ema50,
            rsi14        = rsi,
            rsiOversold  = if (rsi < 30.0) 1.0 else 0.0,
            rsiOverbought= if (rsi > 70.0) 1.0 else 0.0,
            macd         = macd,
            bbPosition   = bbPos.coerceIn(0.0, 1.0),
            atr          = atr,
            volRatio     = volRatio,
        )
    }

    // ── Indicator math ─────────────────────────────────────────────────────

    private fun ema(prices: List<Double>, period: Int): Double {
        if (prices.isEmpty()) return 0.0
        val k = 2.0 / (period + 1)
        return prices.drop(1).fold(prices.first()) { acc, p -> p * k + acc * (1 - k) }
    }

    private fun rsi(prices: List<Double>, period: Int = 14): Double {
        if (prices.size < 2) return 50.0
        val changes = prices.zipWithNext { a, b -> b - a }
        val window  = changes.takeLast(period)
        val gains   = window.filter { it > 0 }.average().takeIf { !it.isNaN() } ?: 0.0
        val losses  = window.filter { it < 0 }.map { -it }.average().takeIf { !it.isNaN() } ?: 1e-9
        return 100.0 - (100.0 / (1.0 + gains / losses))
    }

    private fun atr(highs: List<Double>, lows: List<Double>, closes: List<Double>): Double {
        if (closes.size < 2) return 0.0
        val trs = (1 until minOf(highs.size, lows.size, closes.size)).map { i ->
            maxOf(
                highs[i] - lows[i],
                Math.abs(highs[i] - closes[i - 1]),
                Math.abs(lows[i]  - closes[i - 1]),
            )
        }
        return if (trs.isEmpty()) 0.0 else trs.average()
    }

    private fun congressModifier(transactionType: String?, amountRange: String?): Double {
        val direction = when {
            transactionType?.lowercase()?.contains("purchase") == true ->  1.0
            transactionType?.lowercase()?.contains("sale")     == true -> -1.0
            else -> return 0.0
        }
        val magnitude = when {
            amountRange?.contains("1,000,000") == true -> 1.0
            amountRange?.contains("500,000")   == true -> 0.8
            amountRange?.contains("100,000")   == true -> 0.5
            amountRange?.contains("50,000")    == true -> 0.3
            else -> 0.1
        }
        return direction * magnitude
    }
}
