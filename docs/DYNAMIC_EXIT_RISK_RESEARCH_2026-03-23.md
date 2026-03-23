# Dynamic Exit And Risk Research - 2026-03-23

## Why this note exists

The immediate trigger for this note is the live canary behavior observed on
`2026-03-23`:

- the active canary runtime was bound to run
  `20260322T184122Z-s42-d7180469`
- that run had already failed acceptance on runtime parity
- the live risk plan for `KRW-ORDER` still allowed roughly:
  - hold horizon: `9` bars (`45` minutes)
  - take-profit: about `10.99%`
  - stop-loss: about `6.59%`
  - trailing: off
- while the position was already materially below entry, the risk manager did
  not exit because the plan had not hit `TP`, `SL`, `TRAILING`, or `TIMEOUT`

This is not just a parameter problem. It is a design problem.

The current exit contract is too close to:

`volatility * sqrt(horizon) -> static TP/SL -> wait`

That is not a smart intraday risk-management policy.

## Current system diagnosis

### Current code path

The current exit policy is assembled through these paths:

- `autobot/strategy/model_alpha_v1.py`
  - `ModelAlphaExitSettings`
  - `build_model_alpha_exit_plan_payload`
  - `_resolve_runtime_risk_exit_thresholds`
  - `_reprice_position_exit_plan`
- `autobot/models/trade_action_policy.py`
  - `_resolve_template_thresholds`
- `autobot/risk/live_risk_manager.py`
  - exit decision currently resolves mainly through `TP / SL / TRAILING / TIMEOUT`
- `autobot/live/model_risk_plan.py`
  - projects the entry-time plan into a persisted live risk plan
- `autobot/live/model_alpha_projection.py`
  - backfills or repairs risk plans from entry intents

### Structural issue

The main problem is that the system currently mixes up three different concepts:

1. volatility scale
2. reachable favorable move before horizon
3. optimal exit boundary under execution frictions

They are not the same object.

A volatility estimate such as `sigma * sqrt(T)` is a risk scale. It is not,
by itself, a valid take-profit target.

If a 45-minute position gets a `10%+` TP because volatility is high, the
result is usually not "smart patience". It is a disguised instruction to
ignore mean reversion and wait for timeout.

### Why the current behavior is unintelligent

The present controller is mostly threshold-triggered:

- if price reaches TP -> exit
- if price reaches SL -> exit
- if price reaches trailing floor -> exit
- if time expires -> exit

What it does not do:

- estimate continuation value versus immediate liquidation value
- estimate whether the target is actually reachable before timeout
- estimate whether alpha has already decayed
- estimate whether a passive exit still has acceptable fill probability
- tighten aggressiveness when the path starts to revert but has not yet hit SL

This makes the policy mechanically consistent but economically naive.

## Literature review

The papers below suggest a much smarter architecture than static
entry-time thresholds.

### 1. Do not optimize stop-loss / take-profit by naive backtest search

Marcos Lopez de Prado, "Optimal Trading Rules Without Backtesting" (2014)

- Source: https://ssrn.com/abstract=2502613
- Key point: blindly calibrating profit-taking and stop-loss levels with
  historical backtests causes backtest overfitting.
- Practical implication for us:
  - threshold search should not be treated as a free hyperparameter loop
  - exit rules should be tied to a state model and an economic objective,
    not just brute-force backtest maximization

Wilson Ma, Guy Morita, Kira Detko, "Re-Examining the Hidden Costs of the
Stop-Loss" (2008)

- Source: https://ssrn.com/abstract=1123362
- Key point: stop-loss rules change the return distribution, but hidden costs
  can offset their intuitive benefits.
- Practical implication for us:
  - a stop is not automatically good
  - TP, SL, and trailing must be judged net of slippage, fees, adverse
    selection, and opportunity cost

James Battle, "A Simple Trading Strategy with a Stop-Loss and Take-Profit
Order" (2025)

- Source: https://ssrn.com/abstract=5859402
- Key point: optimal stop placement depends on the strategy distribution and
  can be approximated analytically under assumptions, rather than being picked
  as a fixed folklore percentage.
- Practical implication for us:
  - a stop should be tied to the underlying return process and expected edge
  - a fixed heuristic threshold is only a placeholder

### 2. Alpha decay and transaction costs should drive the exit policy

Chutian Ma, Paul Smith, "On the Effect of Alpha Decay and Transaction Costs on
the Multi-period Optimal Trading Strategy" (2025)

- Source: https://arxiv.org/abs/2502.04284
- Key point: with transaction costs, greedily maximizing immediate reward is
  not optimal; the right policy is multi-period and explicitly depends on
  signal decay.
- Practical implication for us:
  - the correct exit question is not "did we hit TP yet?"
  - it is "is the continuation value of staying in the trade still positive
    after costs?"

This is the most important conceptual correction for our current design.

### 3. Liquidity regime and hidden market state must alter aggressiveness

Guiyuan Ma, Chi Chung Siu, S. C. P. Yam, Zeyu Zhou,
"Dynamic Trading with Markov Liquidity Switching" (2022/2023)

- Source: https://ssrn.com/abstract=4202586
- Key point: optimal trading aggressiveness should depend on the current
  liquidity regime, resilience, and price impact.
- Practical implication for us:
  - exit mode cannot be static
  - when the book regime changes, the right action may shift from passive
    exit to aggressive cleanup

Etienne Chevalier, Yadh Hafsi, Vathana Ly Vath,
"Optimal Execution under Incomplete Information" (2024)

- Source: https://arxiv.org/abs/2411.04616
- Key point: the hidden liquidity state should be filtered online and the
  resulting control should be solved as a dynamic stopping / impulse problem.
- Practical implication for us:
  - exit should be formulated as a partially observed control problem
  - we should update beliefs about liquidity and alpha state during the trade,
    not freeze them at entry

### 4. Fill probability and cleanup cost are first-class inputs

Alvaro Arroyo, Alvaro Cartea, Fernando Moreno-Pino, Stefan Zohren,
"Deep Attentive Survival Analysis in Limit Order Books" (2023)

- Source: https://arxiv.org/abs/2306.05479
- Key point: fill time distributions for passive orders are strongly
  state-dependent and can be learned with survival models.
- Practical implication for us:
  - passive versus aggressive exit must depend on predicted fill-time
    distributions
  - "post and wait" is not intelligent without a fill survival model

Timothee Fabre, Vincent Ragel,
"Tackling the Problem of State Dependent Execution Probability" (2023)

- Source: https://ssrn.com/abstract=4509063
- Key point: fill probability and cleanup cost are strongly state-dependent,
  and realistic backtesting should avoid hypothetical orders that ignore this.
- Practical implication for us:
  - live and paper should share the same fill and cleanup-cost model
  - exit policy should explicitly compare passive-fill value against cleanup
    cost

Jakob Albers, Mihai Cucuringu, Sam Howison, Alexander Y. Shestopaloff,
"The Good, the Bad, and Latency: Exploratory Trading on Bybit and Binance"
(2024)

- Source: https://ssrn.com/abstract=4677989
- Key point: live crypto execution suffers systematic slippage and fail-to-fill
  effects that correlate with latency, volatility, and liquidity.
- Practical implication for us:
  - a smart exit policy must account for latency-sensitive deterioration
  - if favorable exits are latency-sensitive, waiting can convert paper alpha
    into realized loss

### 5. Offline learning and simulators overfit unless context is compressed and realism is high

Chuheng Zhang et al., "Towards Generalizable Reinforcement Learning for Trade
Execution" (2023)

- Source: https://arxiv.org/abs/2307.11685
- Key point: offline execution RL overfits because context space is large and
  historical contexts are limited; high-fidelity simulation and compact state
  representations are required.
- Practical implication for us:
  - if we learn dynamic exit or execution policies, we must use compact state,
    realistic simulator assumptions, and out-of-sample discipline

Karush Suri et al., "TradeR: Practical Deep Hierarchical Reinforcement
Learning for Trade Execution" (2021)

- Source: https://arxiv.org/abs/2104.00620
- Key point: execution should be hierarchical and robust to abrupt dynamics,
  not monolithic.
- Practical implication for us:
  - it is reasonable to separate:
    - strategic hold/exit decision
    - order aggressiveness decision
    - price placement decision

Feiyang Pan et al., "Learn Continuously, Act Discretely" (2022)

- Source: https://arxiv.org/abs/2207.11152
- Key point: the control state may be continuous, but the actual action must
  respect discrete limit-price choices.
- Practical implication for us:
  - the policy may compute a continuous urgency score, but the final order
    action must still choose among discrete modes like passive / join / cross

## What the literature says our current design is missing

The literature strongly suggests we should model four things separately:

1. directional alpha and alpha decay
2. path risk until remaining horizon
3. fill survival and cleanup cost
4. market/liquidity regime

Our current design mostly models:

1. directional alpha
2. some path volatility proxy

and only partially models:

3. fill / cleanup through execution policy and audit contracts
4. regime through operational overlay and micro-quality heuristics

That is why it behaves like a threshold machine instead of a dynamic controller.

## What a smarter design looks like

### Principle

Do not treat TP, SL, and trailing as static numbers chosen at entry.

Treat them as outputs of a value-based controller with event-time updates.

### Core objects to predict

For every open trade, at time `t`, with remaining horizon `h`, predict:

1. `MFE_h(s_t)`
   - conditional distribution of maximum favorable excursion before timeout
2. `MAE_h(s_t)`
   - conditional distribution of maximum adverse excursion before timeout
3. `alpha_survival_h(s_t)`
   - probability that continuation alpha remains positive after waiting
4. `fill_survival(delta, h, s_t)`
   - probability a passive or near-passive exit fills within horizon
5. `cleanup_cost(delta, s_t)`
   - expected cost if the passive exit misses and we must cross later

Here `s_t` should include:

- elapsed bars since entry
- model probability and raw score
- current unrealized return
- MFE since entry
- MAE since entry
- realized and expected spread / depth / imbalance
- recent order flow imbalance
- current queue and fill state
- volatility state
- latency and update freshness features

### Controller objective

At each event, compare:

- immediate exit value now
- continuation value of staying

Conceptually:

- `ImmediateExitValue = expected proceeds from exiting now - fees - slippage`
- `ContinuationValue = expected future value from waiting and following the best
   future exit policy - inventory risk - non-fill risk - alpha decay penalty`

Then:

- if `ImmediateExitValue >= ContinuationValue - hysteresis_margin`, exit now
- else continue holding

This is much more intelligent than waiting for a static TP or SL to be hit.

### How TP should be set

TP should be reachability-aware, not just volatility-aware.

Better:

- `TP_t = min(reachable_TP_quantile, economic_TP_cap)`

Where:

- `reachable_TP_quantile` comes from conditional `MFE` quantiles for the
  remaining horizon
- `economic_TP_cap` comes from the expected edge net of costs and alpha decay

This prevents absurd targets like `10.99%` in a `45` minute holding plan unless
the historical conditional path distribution genuinely supports such moves.

### How SL should be set

SL should be loss-budget-aware and tail-aware.

Better:

- `SL_t = min(loss_budget_cap, MAE_tail_quantile_cap)`

This means the stop is not just a symmetric twin of TP. It is bounded by what
the strategy can economically afford and by what the path-risk model says is a
meaningful adverse move for that state.

### How trailing should be used

Trailing should not be always off until timeout. But it also should not be
always on from entry.

Better:

- arm trailing only after realized `MFE` exceeds a minimum floor
- make trailing distance depend on:
  - current alpha survival
  - current fill survival
  - current liquidity regime
  - realized excursion since entry

If the trade has made enough favorable movement and current continuation value
is weakening, trailing should activate to preserve edge.

### How frequently should the plan update?

The user intuition is directionally correct: the policy should be refreshed
continuously as new ticks and order-book events arrive.

But the final order or plan should not be mutated on every raw tick.

The right design is:

- recompute the latent exit state on every event or micro-batch
- mutate the live order or risk plan only if the value difference is material

Use update hysteresis such as:

- minimum utility delta in bps
- minimum probability delta for fill or alpha survival
- minimum elapsed-time bucket change
- minimum price-distance change in ticks
- minimum replacement interval

This avoids noisy overreaction while still making the strategy genuinely
adaptive.

## Recommended target architecture for Autobot

### Layer 1. Entry alpha model

Keep the directional entry model, but explicitly pass its uncertainty and decay
state downstream.

### Layer 2. Path-risk model

Train conditional models for:

- `MFE`
- `MAE`
- expected forced-exit return at horizons `1, 2, 3, 6, 9, 12` bars
- probability that the trade is still worth holding after `k` more bars

These should be fit on certification / out-of-sample paths, not training paths.

Good model families:

- gradient boosted quantile models
- conformalized quantile regressors
- discrete-time hazard / survival models
- distributional models if sample size is adequate

### Layer 3. Execution survival model

Train state-dependent models for:

- passive fill probability
- expected fill time
- expected cleanup cost after miss
- slippage as a function of urgency, spread, depth, and latency

This is where papers by Arroyo et al. and Fabre/Ragel are directly relevant.

### Layer 4. Value-based exit controller

Use the above models to choose:

- keep holding
- post passive exit
- join
- cross / IOC cleanup

The controller should output:

- dynamic TP
- dynamic SL
- trailing state
- remaining timeout
- exit order aggression

### Layer 5. Hard supervisory guardrails

Even after the smart controller exists, keep guardrails:

- absolute max TP for intraday horizons
- absolute max SL for intraday horizons
- max stale-position lifetime
- max cleanup delay after reversal evidence

These are not the main policy. They are circuit breakers against model error.

## Offline / paper / live parity requirements

This is critical.

The same exit logic must run in:

- training certification
- paper
- live

with the same:

- state features
- event clock
- fill / cleanup model
- latency assumptions
- path labels
- timeout semantics

If offline uses static end-of-bar exits while live uses event-driven exits, the
training target is wrong.

If paper assumes perfect cleanup while live suffers misses, the policy target is
wrong.

If runtime re-prices exits but the offline trade-action simulator does not, the
policy target is wrong.

## Concrete recommendations for this codebase

### Stop doing

1. Do not let `tp_vol_multiplier * sigma * sqrt(horizon)` directly become live
   TP with no reachability model.
2. Do not freeze the entry-time risk plan unless no new information exists.
3. Do not treat paper and live execution costs as separate afterthoughts.
4. Do not let runtime parity approve or reject without pathwise exit realism.

### Start doing

1. Add pathwise OOS labels:
   - max favorable return before each horizon
   - max adverse return before each horizon
   - forced-exit return at each horizon
   - continuation-value proxy after each elapsed bucket
2. Learn state-dependent reachable TP and bounded SL from those labels.
3. Add a fill-survival and cleanup-cost model shared by paper and live.
4. Recompute exit value on every market-data event, but only mutate orders when
   hysteresis conditions are met.
5. Separate:
   - holding decision
   - risk-bound update
   - order aggressiveness update
6. Fix stale local state:
   - closed positions must not remain visible as open risk exposure

## Recommended rollout order

1. Build the pathwise dataset from certification and recent paper/live runs.
2. Replace volatility-scaled TP with reachable-TP quantiles.
3. Replace static SL with bounded MAE / tail-aware stop logic.
4. Add continuation-value re-evaluation in paper first.
5. Add fill-survival and cleanup-cost estimation to exit action selection.
6. Deploy to canary only after parity checks use the same controller.

## Practical answer to the original question

Should TP / SL / trailing change as live data arrives?

Yes, but not by blindly mutating thresholds on every raw tick.

The right answer is:

- state refresh: yes, event by event
- plan / order mutation: only when the estimated optimal action changes
  materially

That is how you get:

- intelligent risk control
- realistic profit capture
- timely exits on reversals
- and stable live behavior without noisy thrashing

## Best high-signal sources used

- Lopez de Prado, "Optimal Trading Rules Without Backtesting" (2014)
  - https://ssrn.com/abstract=2502613
- Ma, Morita, Detko, "Re-Examining the Hidden Costs of the Stop-Loss" (2008)
  - https://ssrn.com/abstract=1123362
- Battle, "A Simple Trading Strategy with a Stop-Loss and Take-Profit Order" (2025)
  - https://ssrn.com/abstract=5859402
- Ma, Smith, "On the Effect of Alpha Decay and Transaction Costs on the Multi-period Optimal Trading Strategy" (2025)
  - https://arxiv.org/abs/2502.04284
- Ma, Siu, Yam, Zhou, "Dynamic Trading with Markov Liquidity Switching" (2022/2023)
  - https://ssrn.com/abstract=4202586
- Chevalier, Hafsi, Ly Vath, "Optimal Execution under Incomplete Information" (2024)
  - https://arxiv.org/abs/2411.04616
- Arroyo, Cartea, Moreno-Pino, Zohren, "Deep Attentive Survival Analysis in Limit Order Books" (2023)
  - https://arxiv.org/abs/2306.05479
- Fabre, Ragel, "Tackling the Problem of State Dependent Execution Probability" (2023)
  - https://ssrn.com/abstract=4509063
- Albers, Cucuringu, Howison, Shestopaloff, "The Good, the Bad, and Latency" (2024)
  - https://ssrn.com/abstract=4677989
- Zhang et al., "Towards Generalizable Reinforcement Learning for Trade Execution" (2023)
  - https://arxiv.org/abs/2307.11685
- Suri et al., "TradeR: Practical Deep Hierarchical Reinforcement Learning for Trade Execution" (2021)
  - https://arxiv.org/abs/2104.00620
- Pan et al., "Learn Continuously, Act Discretely" (2022)
  - https://arxiv.org/abs/2207.11152
