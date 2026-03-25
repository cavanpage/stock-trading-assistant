package com.tradingassistant.operators

import com.tradingassistant.model.EventType
import com.tradingassistant.model.MarketEvent
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class FeatureWindowFunctionTest {

    private fun makeHarness(): KeyedOneInputStreamOperatorTestHarness<String, MarketEvent, *> {
        val fn = FeatureWindowFunction()
        val op = KeyedProcessOperator(fn)
        return KeyedOneInputStreamOperatorTestHarness(op, { e: MarketEvent -> e.symbol }, org.apache.flink.api.common.typeinfo.Types.STRING)
            .also { it.open() }
    }

    @Test
    fun `no output before MIN_BARS threshold`() {
        val harness = makeHarness()
        repeat(25) { i ->
            harness.processElement(priceEvent(close = 100.0 + i), (i * 60_000).toLong())
        }
        assertEquals(0, harness.extractOutputValues().size, "Should not emit before 26 bars")
    }

    @Test
    fun `emits feature vector after sufficient bars`() {
        val harness = makeHarness()
        repeat(30) { i ->
            harness.processElement(priceEvent(close = 100.0 + i * 0.5), (i * 60_000).toLong())
        }
        val output = harness.extractOutputValues()
        assert(output.isNotEmpty()) { "Should emit at least one feature vector after 30 bars" }
        val fv = output.last()
        assertNotNull(fv)
        assertEquals("AAPL", fv.symbol)
    }

    @Test
    fun `congress modifier is applied correctly`() {
        val harness = makeHarness()
        // Build up history first
        repeat(30) { i ->
            harness.processElement(priceEvent(close = 150.0 + i), (i * 60_000).toLong())
        }
        // Congress purchase event
        harness.processElement(
            MarketEvent(
                eventType = EventType.CONGRESS,
                symbol = "AAPL",
                timestampMs = 99_000_000L,
                transactionType = "Purchase",
                amountRange = "\$100,000 - \$250,000",
                memberName = "Nancy Pelosi",
            ),
            99_000_000L,
        )
        // Trigger another price bar to get a new feature vector
        harness.processElement(priceEvent(close = 180.0), 100_000_000L)

        val output = harness.extractOutputValues()
        val last = output.last()
        assert(last.congressModifier > 0.0) { "Purchase should produce positive congress modifier" }
    }

    private fun priceEvent(close: Double) = MarketEvent(
        eventType  = EventType.PRICE,
        symbol     = "AAPL",
        timestampMs= System.currentTimeMillis(),
        open       = close - 0.5,
        high       = close + 1.0,
        low        = close - 1.0,
        close      = close,
        volume     = 1_000_000.0,
    )
}
