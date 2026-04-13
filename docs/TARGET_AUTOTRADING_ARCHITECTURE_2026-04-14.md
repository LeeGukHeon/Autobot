# TARGET AUTOTRADING ARCHITECTURE 2026-04-14

## 0. Purpose

This document records the intended target shape of the automated trading system.

The target is not:

- a brittle nightly batch that only trades when the exact daily top-n collection state happens to be complete

The target is:

- a broadly trained market model
- a real-time top-market scanner
- a live execution layer that only deploys capital when the expected opportunity is strong enough

In plain language:

- train on a lot of historical market data
- watch the current top liquid markets in real time
- detect when a market looks worth buying
- execute only the best opportunities under strict risk/execution controls

## 1. Target Mental Model

The intended bot has four distinct layers:

1. Research / training layer
2. Live market scanning layer
3. Opportunity selection layer
4. Execution / risk layer

These layers must be loosely coupled.

The current codebase still mixes parts of these layers too tightly through shared nightly refresh and acceptance contracts.

The target architecture must separate them.

## 2. Training Layer

The training layer must be:

- broad
- stable
- slow-moving

It must not depend directly on the exact set of markets that happened to be in the live top-n on a single day.

Target rule:

- define a stable research universe
- collect and retain enough historical data for that universe
- train on long windows across many regimes

Training data should come from:

- `candles_api_v1`
- `candles_second_v1`
- `ws_candle_v1`
- `micro_v1`
- `lob30_v1`
- `sequence_v1`
- `private_execution_v1`
- `features_v4`

The training layer should answer:

- what setups tend to work
- under which market regimes
- with what expected return / risk / fill behavior

## 3. Live Scanning Layer

The live scanner must be:

- dynamic
- fast
- tolerant to changing market leadership

Target rule:

- continuously rank current liquid markets
- compute online market state for those markets
- keep scanning even when no trade is taken

The live scanner should not be blocked by whether the historical nightly training universe changed by one or two markets.

It should produce:

- current top liquid markets
- live feature rows
- microstructure quality state
- market readiness state

## 4. Opportunity Selection Layer

The opportunity layer is where the trained model meets the live market stream.

The model should not only output a naive buy/sell flag.

The preferred output is expectation-oriented:

- expected return
- expected downside / ES
- fill probability
- expected shortfall
- uncertainty
- lower-confidence alpha score

The system should then decide:

- is this opportunity good enough
- is it good enough after fees and execution friction
- is it good enough relative to other markets right now

Target rule:

- evaluate many markets
- select very few
- trade only the highest-quality opportunities

## 5. Execution And Risk Layer

Execution must be separate from prediction.

The prediction layer says:

- this looks profitable enough

The execution layer says:

- can we actually enter
- at what aggressiveness
- with what size
- with what stop / take-profit / timeout policy

Risk must remain a first-class layer:

- position sizing
- max concurrent positions
- stop / timeout / trailing logic
- breaker logic
- canary / rollout gating

Target rule:

- prediction chooses where opportunity exists
- execution chooses how to attempt entry
- risk chooses how much capital is allowed

## 6. Universe Separation

The most important structural change is universe separation.

The target system needs three different universes:

1. Research universe
   - broad
   - relatively stable
   - used for historical training and evaluation

2. Live scan universe
   - dynamic
   - current high-liquidity markets
   - recalculated frequently

3. Tradeable universe
   - intersection of:
     - live scan universe
     - model-supported universe
     - data-complete universe
     - execution-eligible universe

This avoids the failure mode where:

- every nightly change in top-n also changes the effective trainable/tradeable universe

## 7. Data Reuse Principle

The target system must treat every upstream dataset as an independent reusable source.

The rule is:

- reuse if complete
- refresh if stale
- rebuild only what is missing

Never prefer:

- rerunning the whole chain just because one source is incomplete

This applies to:

- candles
- second candles
- micro
- lob
- sequence tensors
- private execution labels
- feature datasets

The frozen snapshot should be the final result of completeness checks, not the mechanism used to discover late failures.

## 8. What The Current System Is Missing

Relative to the target architecture, the current system still has these problems:

- nightly close and training are too tightly coupled
- source refresh and freeze are too expensive
- high-tf completeness can fail late
- historical refresh still redoes too much work
- `lob30` historical cost is too close to live runtime cost
- training universe and live scan universe are not cleanly separated

## 9. Target Operating Flow

The intended long-run operating flow is:

1. maintain broad source datasets continuously
2. incrementally refresh only missing/stale slices
3. verify source completeness for the required train/eval window
4. freeze a ready snapshot
5. train/update expert models and fusion model
6. validate runtime deployability
7. live scanner watches current liquid markets
8. model scores current opportunities
9. execution/risk layer decides whether to submit orders

This is different from:

- "collect just enough top-n data for today, then hope training and trading line up"

## 10. Migration Principle

The path from the current system to the target system should be incremental.

Recommended migration order:

1. make every source dataset incrementally reusable
2. add explicit completeness contracts
3. reduce historical rebuild cost
4. separate training universe from live scan universe
5. strengthen expectation-based live selection
6. keep execution and risk as separate deploy gates

## 11. Practical Summary

The intended bot is:

- trained broadly
- scanning dynamically
- choosing selectively
- executing conservatively

In simpler terms:

- learn from many markets
- watch the strongest current markets
- buy only when the model thinks the edge is real
- let execution/risk decide whether the trade is actually safe enough to take
