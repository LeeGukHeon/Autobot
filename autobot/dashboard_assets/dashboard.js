(function () {
  const REASON_TEXT = {
    BACKTEST_ACCEPTANCE_FAILED: "백테스트 기준 미통과",
    TRAINER_EVIDENCE_REQUIRED_FAILED: "학습 증거 미통과",
    PAPER_SOAK_SKIPPED: "내부 소크 생략",
    OFFLINE_NOT_CANDIDATE_EDGE: "오프라인 우위 없음",
    SPA_LIKE_NOT_CANDIDATE_EDGE: "SPA 유사 검정 우위 없음",
    WHITE_RC_NOT_CANDIDATE_EDGE: "White 검정 우위 없음",
    HANSEN_SPA_NOT_CANDIDATE_EDGE: "Hansen 검정 우위 없음",
    EXECUTION_NOT_CANDIDATE_EDGE: "실행 기준 우위 없음",
    DUPLICATE_CANDIDATE: "기존 챔피언과 동일",
    LIVE_BREAKER_ACTIVE: "라이브 브레이커 활성",
    MODEL_POINTER_DIVERGENCE: "모델 포인터 불일치",
    WS_PUBLIC_STALE: "WS 수집 오래됨",
    UNKNOWN_POSITIONS_DETECTED: "거래소 포지션 불일치",
    SMALL_ACCOUNT_CANARY_MULTIPLE_ACTIVE_MARKETS: "카나리아 슬롯 초과",
    EXTERNAL_OPEN_ORDERS_DETECTED: "외부 미체결 주문 감지",
    LOCAL_POSITION_MISSING_ON_EXCHANGE: "거래소 잔고와 불일치",
    SKIPPED_SINGLE_SLOT_ACTIVE_ORDER: "이미 열린 주문이 있어 건너뜀",
    REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST: "비용 차감 후 기대 우위 없음",
    INSUFFICIENT_FREE_BALANCE: "가용 잔고 부족",
    FEE_RESERVE_INSUFFICIENT: "수수료 여유 부족",
    DUST_REMAINDER: "잔량이 너무 작음",
    BELOW_MIN_TOTAL: "최소 주문 금액 미만",
    INVALID_PARAMETER: "주문 파라미터 오류",
    DUPLICATE_EXIT_ORDER: "같은 매도 주문이 이미 있음",
    CANARY_SLOT_UNAVAILABLE: "카나리아 슬롯 사용 중"
  };

  const POLICY_TEXT = {
    active: "활성",
    inactive: "비활성",
    failed: "실패",
    waiting: "대기",
    dead: "정지",
    exited: "종료",
    running: "실행 중",
    canary: "카나리아",
    shadow: "그림자",
    live: "정식",
    rank_effective_quantile: "순위 컷오프",
    raw_threshold: "절대 컷오프",
    hold: "시간 보유",
    risk: "TP·SL·추적",
    none: "없음",
    ACTIVE: "진행 중",
    TRIGGERED: "매도 대기",
    EXITING: "매도 진행 중",
    CLOSED: "종료 완료",
    wait: "대기",
    done: "완료",
    cancel: "취소",
    bid: "매수",
    ask: "매도",
    limit: "지정가",
    price: "시장가(금액)",
    market: "시장가",
    MODEL_ALPHA_ENTRY_V1: "진입",
    MODEL_ALPHA_EXIT_TIMEOUT: "시간 종료",
    MODEL_ALPHA_EXIT_TP: "익절",
    MODEL_ALPHA_EXIT_SL: "손절",
    MODEL_ALPHA_EXIT_TRAILING: "추적 매도"
  };

  const SERVICE_LABELS = {
    paper_champion: "챔피언 페이퍼",
    paper_challenger: "챌린저 페이퍼",
    ws_public: "WS 수집기",
    live_main: "메인 라이브",
    live_candidate: "후보 카나리아",
    spawn_service: "spawn 서비스",
    promote_service: "promote 서비스",
    spawn_timer: "spawn 타이머",
    promote_timer: "promote 타이머"
  };

  const INITIAL_SNAPSHOT = JSON.parse(document.getElementById("initial-snapshot").textContent || "{}");

  function esc(value) {
    return String(value == null ? "-" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function maybe(value, fallback = "-") {
    return value == null || value === "" ? fallback : value;
  }

  function translate(value) {
    if (value == null || value === "") return "-";
    return REASON_TEXT[value] || POLICY_TEXT[value] || String(value);
  }

  function unique(values) {
    return [...new Set((values || []).filter(Boolean))];
  }

  function toNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function fmtNumber(value, digits = 2) {
    const num = toNumber(value);
    return num == null ? "-" : num.toLocaleString("ko-KR", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits
    });
  }

  function fmtMoney(value) {
    const num = toNumber(value);
    return num == null ? "-" : `${Math.round(num).toLocaleString("ko-KR")}원`;
  }

  function fmtPct(value) {
    const num = toNumber(value);
    return num == null ? "-" : `${num.toLocaleString("ko-KR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
  }

  function fmtBps(value) {
    const num = toNumber(value);
    return num == null ? "-" : `${num.toLocaleString("ko-KR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}bps`;
  }

  function shortRun(value) {
    const text = String(value || "").trim();
    if (!text) return "-";
    return text.length > 18 ? `${text.slice(0, 18)}…` : text;
  }

  function shortPath(value) {
    const text = String(value || "").trim();
    if (!text) return "-";
    return text.length > 64 ? `…${text.slice(-64)}` : text;
  }

  function coerceTs(value) {
    const num = toNumber(value);
    if (num == null) return null;
    return num > 10000000000 ? num : num * 1000;
  }

  function fmtDateTime(value) {
    const ts = coerceTs(value);
    if (ts != null) return new Date(ts).toLocaleString("ko-KR", { hour12: false });
    const text = String(value || "").trim();
    if (!text) return "-";
    const parsed = Date.parse(text);
    return Number.isNaN(parsed) ? text : new Date(parsed).toLocaleString("ko-KR", { hour12: false });
  }

  function fmtAge(value) {
    const ts = coerceTs(value);
    if (ts == null) return "-";
    const sec = Math.max(0, (Date.now() - ts) / 1000);
    if (sec < 60) return `${sec.toFixed(0)}초 전`;
    if (sec < 3600) return `${(sec / 60).toFixed(0)}분 전`;
    return `${(sec / 3600).toFixed(1)}시간 전`;
  }

  function boolLabel(value) {
    if (value === true) return "예";
    if (value === false) return "아니오";
    return "-";
  }

  function statusClass(kind) {
    if (kind === "good") return "status-pill status-good";
    if (kind === "warn") return "status-pill status-warn";
    if (kind === "bad") return "status-pill status-bad";
    return "status-pill status-neutral";
  }

  function pill(label, value, kind = "neutral") {
    return `<span class="${statusClass(kind)}">${esc(label)} · ${esc(value)}</span>`;
  }

  function metric(key, value) {
    return `<div class="metric"><div class="k">${esc(key)}</div><div class="v">${esc(value)}</div></div>`;
  }

  function kv(key, value) {
    return `<div class="kv"><div class="k">${esc(key)}</div><div class="v">${esc(value)}</div></div>`;
  }

  function empty(message) {
    return `<div class="empty">${esc(message)}</div>`;
  }

  function card(title, body, extraClass = "") {
    return `<article class="${extraClass ? `${extraClass} ` : ""}detail-box"><h4>${esc(title)}</h4>${body}</article>`;
  }

  function noteCard(title, text, kind = "neutral") {
    return `<article class="alert-card"><div class="row"><h4>${esc(title)}</h4>${pill("메모", kind === "bad" ? "주의" : kind === "warn" ? "참고" : "요약", kind)}</div><p>${esc(text)}</p></article>`;
  }

  function setError(message) {
    const node = document.getElementById("fetch-error");
    if (!message) {
      node.hidden = true;
      node.textContent = "";
      return;
    }
    node.hidden = false;
    node.textContent = message;
  }

  function setTab(nextTab) {
    document.querySelectorAll(".tab-button").forEach((button) => {
      button.classList.toggle("active", button.dataset.tab === nextTab);
      button.setAttribute("aria-selected", button.dataset.tab === nextTab ? "true" : "false");
    });
    document.querySelectorAll(".pane").forEach((pane) => {
      pane.hidden = pane.dataset.pane !== nextTab;
    });
  }

  function bindTabs() {
    document.getElementById("tab-bar").addEventListener("click", (event) => {
      const button = event.target.closest(".tab-button");
      if (!button) return;
      event.preventDefault();
      setTab(button.dataset.tab);
    });
  }

  function renderMeta(snapshot) {
    document.getElementById("generated-at").textContent = fmtDateTime(snapshot.generated_at);
    document.getElementById("project-root").textContent = snapshot.project_root || "-";
    const system = snapshot.system || {};
    const projectGb = Number(system.project_used_bytes || 0) / (1024 ** 3);
    const fsUsedGb = Number(system.used_bytes || 0) / (1024 ** 3);
    const totalGb = Number(system.total_bytes || 0) / (1024 ** 3);
    document.getElementById("storage-summary").textContent = `프로젝트 ${fmtNumber(projectGb, 1)} GB · 전체 ${fmtNumber(fsUsedGb, 1)} / ${fmtNumber(totalGb, 1)} GB`;
  }

  function renderOverview(snapshot) {
    const acceptance = (snapshot.training || {}).acceptance || {};
    const liveStates = (snapshot.live || {}).states || [];
    const candidateLive = liveStates.find((item) => String(item.label || "").includes("후보")) || {};
    const challenger = snapshot.challenger || {};

    const headline =
      acceptance.overall_pass === true ? "이번 후보는 통과했습니다." :
      acceptance.overall_pass === false ? "이번 후보는 탈락했고 챔피언 유지입니다." :
      "최신 검증 결과를 기다리는 중입니다.";
    const subhead =
      Number(candidateLive.positions_count || 0) > 0
        ? `후보 카나리아는 현재 ${candidateLive.positions_count}개 포지션을 보유 중입니다.`
        : challenger.started
          ? "챌린저 생성 절차는 실행됐지만 아직 활성 포지션은 없습니다."
          : "현재는 챔피언 중심 운영 상태입니다.";

    document.getElementById("overview-headline").textContent = headline;
    document.getElementById("overview-subhead").textContent = subhead;
    document.getElementById("overview-kpis").innerHTML = [
      metric("후보 run", shortRun(acceptance.candidate_run_id)),
      metric("판정", acceptance.overall_pass === true ? "통과" : acceptance.overall_pass === false ? "탈락" : "-"),
      metric("후보 라이브 포지션", maybe(candidateLive.positions_count, "0")),
      metric("후보 리스크 플랜", maybe(candidateLive.active_risk_plans_count, "0"))
    ].join("");

    const services = snapshot.services || {};
    document.getElementById("services-grid").innerHTML = Object.entries(services).map(([key, svc]) => {
      const label = SERVICE_LABELS[key] || key;
      const active = String(svc.active_state || "").toLowerCase();
      const sub = String(svc.sub_state || "").toLowerCase();
      const kind = active === "active" ? "good" : active === "failed" ? "bad" : "warn";
      return `<article class="service-card"><div class="row"><h4>${esc(label)}</h4>${pill("상태", `${translate(active)} / ${translate(sub)}`, kind)}</div><div class="kv-grid">${kv("최근 시작", fmtDateTime(svc.started_at))}${kv("다음 실행", fmtDateTime(svc.next_run_at))}</div></article>`;
    }).join("") || empty("표시할 서비스가 없습니다.");

    const notes = [];
    if ((acceptance.reasons || []).length) notes.push(noteCard("직접 사유", unique(acceptance.reasons).map(translate).join(" / "), "warn"));
    if ((acceptance.trainer_reasons || []).length) notes.push(noteCard("학습 증거", unique(acceptance.trainer_reasons).map(translate).join(" / "), "warn"));
    if (challenger.reason) notes.push(noteCard("챌린저 미기동 사유", translate(challenger.reason), "warn"));
    liveStates.forEach((liveState) => {
      const reasons = unique((liveState.active_breakers || []).map((item) => item.reason || item.code || item.name));
      if (reasons.length) notes.push(noteCard(`${liveState.label} 브레이커`, reasons.map(translate).join(" / "), "bad"));
    });
    document.getElementById("alerts-grid").innerHTML = notes.length ? notes.join("") : noteCard("현재 상태", "즉시 확인이 필요한 경고는 없습니다.");
  }

  function renderTraining(snapshot) {
    const acceptance = (snapshot.training || {}).acceptance || {};
    const training = snapshot.training || {};
    const challenger = snapshot.challenger || {};
    const summary = unique([...(acceptance.reasons || []), ...(acceptance.trainer_reasons || [])]).map(translate).join(" / ") || "핵심 사유 없음";

    document.getElementById("training-headline").textContent =
      acceptance.overall_pass === true ? "이번 후보는 통과했습니다." :
      acceptance.overall_pass === false ? "이번 후보는 탈락했습니다." :
      "최신 어셉턴스 결과가 없습니다.";
    document.getElementById("training-subhead").textContent = summary;
    document.getElementById("training-kpis").innerHTML = [
      metric("후보 run", shortRun(acceptance.candidate_run_id)),
      metric("배치 날짜", maybe(acceptance.batch_date)),
      metric("판정 기준", translate(acceptance.decision_basis)),
      metric("완료 시각", fmtDateTime(acceptance.completed_at || acceptance.generated_at))
    ].join("");

    document.getElementById("training-details").innerHTML = [
      card("이번 후보", `<div class="kv-grid">${kv("모델 패밀리", maybe(acceptance.model_family))}${kv("후보 폴더", shortPath(acceptance.candidate_run_dir))}${kv("직전 챔피언", shortRun(acceptance.champion_before_run_id))}${kv("현재 챔피언", shortRun(acceptance.champion_after_run_id))}${kv("백테스트", boolLabel(acceptance.backtest_pass))}${kv("paper", boolLabel(acceptance.paper_pass))}</div>`),
      card("챌린저 루프", `<div class="kv-grid">${kv("spawn 결과", challenger.started ? "챌린저 기동" : "기동 안 함")}${kv("직접 사유", translate(challenger.reason))}${kv("메모", unique(challenger.acceptance_notes || []).map(translate).join(" / ") || "-")}${kv("리포트", shortPath(challenger.artifact_path))}</div>`)
    ].join("");

    const artifacts = training.candidate_artifacts || {};
    const runtime = artifacts.runtime_recommendations || {};
    const policy = artifacts.selection_policy || {};
    const calibration = artifacts.selection_calibration || {};
    const budget = artifacts.search_budget_decision || {};
    const factor = artifacts.factor_block_selection || {};
    const cpcv = artifacts.cpcv_lite_report || {};
    const wf = artifacts.walk_forward_report || {};

    document.getElementById("artifact-grid").innerHTML = [
      `<article class="artifact-card"><h4>운용 추천</h4><div class="kv-grid">${kv("종료 모드", translate(runtime.recommended_exit_mode))}${kv("보유 bar", maybe(runtime.recommended_hold_bars))}${kv("TP / SL / 추적", `${fmtPct((toNumber(runtime.tp_pct) || 0) * 100)} / ${fmtPct((toNumber(runtime.sl_pct) || 0) * 100)} / ${fmtPct((toNumber(runtime.trailing_pct) || 0) * 100)}`)}</div></article>`,
      `<article class="artifact-card"><h4>선택 정책</h4><div class="kv-grid">${kv("정책 모드", translate(policy.mode))}${kv("기준 키", maybe(policy.threshold_key))}${kv("상위 비율", policy.rank_quantile == null ? "-" : fmtPct(Number(policy.rank_quantile) * 100))}${kv("보정 방법", maybe(calibration.method))}</div></article>`,
      `<article class="artifact-card"><h4>탐색 예산</h4><div class="kv-grid">${kv("예산 결정", maybe(budget.decision_mode))}${kv("booster sweep", maybe(budget.booster_sweep_trials))}${kv("runtime grid", maybe(budget.runtime_grid_mode))}${kv("사유", unique(budget.reasons || []).map(translate).join(" / ") || "-")}</div></article>`,
      `<article class="artifact-card"><h4>강건성 검증</h4><div class="kv-grid">${kv("CPCV 요청", boolLabel(cpcv.requested))}${kv("White comparable", boolLabel(wf.white_rc_comparable))}${kv("Hansen comparable", boolLabel(wf.hansen_spa_comparable))}${kv("trial 수", maybe(wf.selection_search_trial_count))}</div></article>`
    ].join("");
  }

  function renderPaper(snapshot) {
    const rows = (snapshot.paper || {}).recent_runs || [];
    document.getElementById("paper-grid").innerHTML = rows.map((run) => {
      const fillRate = run.fill_rate == null ? "-" : fmtPct(Number(run.fill_rate) * 100);
      return `<article class="paper-card"><div class="row"><h4>${esc(shortRun(run.run_id))}</h4>${pill("워밍업", boolLabel(run.warmup_satisfied), run.warmup_satisfied ? "good" : "warn")}</div><div class="metric-grid">${metric("제출", maybe(run.orders_submitted, "0"))}${metric("체결", maybe(run.orders_filled, "0"))}${metric("체결률", fillRate)}${metric("실현 손익", fmtMoney(run.realized_pnl_quote))}${metric("평가 손익", fmtMoney(run.unrealized_pnl_quote))}${metric("최대 낙폭", fmtPct(run.max_drawdown_pct))}</div><div class="subtle">${esc(maybe(run.feature_provider))} / ${esc(maybe(run.micro_provider))} · ${esc(fmtDateTime(run.updated_at))}</div></article>`;
    }).join("") || empty("최근 페이퍼 런이 없습니다.");
  }

  function statePriority(item) {
    const label = String(item.label || "");
    const base = label.includes("후보") ? 10000 : label.includes("메인") ? 5000 : 0;
    return base +
      (Number(item.positions_count || 0) * 100) +
      (Number(item.open_orders_count || 0) * 50) +
      (Number(item.active_risk_plans_count || 0) * 40) +
      (Number(item.intents_count || 0));
  }

  function describeExit(plan, position, order) {
    if (!plan) return "활성 매도 전략 없음";
    const qty = fmtNumber(position?.base_amount ?? plan.qty, 8);
    const entry = fmtMoney(position?.avg_entry_price ?? plan.entry_price);
    const timeout = plan.timeout_ts_ms ? fmtDateTime(plan.timeout_ts_ms) : "-";
    const mode = translate(plan.exit_mode);
    const status = translate(plan.state);
    const orderPart = order ? `현재 ${fmtMoney(order.price)} 매도 주문 대기 중` : "아직 매도 주문 없음";
    return `${plan.market} 보유 ${qty}개 · 진입가 ${entry} · 전략 ${mode} · 상태 ${status} · 종료 기준 ${timeout} · ${orderPart}`;
  }

  function renderLive(snapshot) {
    const states = [...((snapshot.live || {}).states || [])].sort((a, b) => statePriority(b) - statePriority(a));
    document.getElementById("live-state-list").innerHTML = states.map((liveState) => {
      const runtime = liveState.runtime_health || {};
      const rollout = liveState.rollout_status || {};
      const positions = liveState.positions || [];
      const openOrders = liveState.open_orders || [];
      const riskPlans = liveState.active_risk_plans || [];
      const intents = liveState.recent_intents || [];
      const activeBreakers = unique((liveState.active_breakers || []).map((item) => item.reason || item.code || item.name)).map(translate);
      const primaryPosition = positions[0];
      const primaryPlan = riskPlans.find((item) => item.market === (primaryPosition || {}).market) || riskPlans[0];
      const primaryOrder = openOrders.find((item) => item.market === (primaryPosition || {}).market && item.side === "ask") || openOrders[0];
      const summaryLine = primaryPosition
        ? describeExit(primaryPlan, primaryPosition, primaryOrder)
        : `${liveState.label}는 현재 보유 포지션이 없습니다.`;

      const positionSection = positions.length
        ? positions.map((position) => card(
          position.market || "-",
          `<div class="kv-grid">${kv("보유 수량", `${fmtNumber(position.base_amount, 8)}개`)}${kv("평균 매수가", fmtMoney(position.avg_entry_price))}${kv("원금", fmtMoney((toNumber(position.base_amount) || 0) * (toNumber(position.avg_entry_price) || 0)))}${kv("최근 갱신", fmtDateTime(position.updated_ts))}</div>`,
          "mini-card"
        )).join("")
        : empty("보유 종목이 없습니다.");

      const orderSection = openOrders.length
        ? openOrders.map((order) => card(
          `${order.market || "-"} · ${translate(order.side)} ${translate(order.ord_type)}`,
          `<div class="kv-grid">${kv("주문가", fmtMoney(order.price))}${kv("요청 수량", fmtNumber(order.volume_req, 8))}${kv("체결 수량", fmtNumber(order.volume_filled, 8))}${kv("거래소 상태", translate(order.raw_exchange_state))}${kv("로컬 상태", translate(order.local_state))}${kv("최근 갱신", fmtDateTime(order.updated_ts))}</div>`,
          "mini-card"
        )).join("")
        : empty("미체결 주문이 없습니다.");

      const planSection = riskPlans.length
        ? riskPlans.map((plan) => card(
          `${plan.market || "-"} · ${translate(plan.exit_mode)}`,
          `<div class="kv-grid">${kv("플랜 상태", translate(plan.state))}${kv("플랜 source", maybe(plan.plan_source))}${kv("연결 intent", shortRun(plan.source_intent_id))}${kv("익절", plan.tp_enabled ? fmtPct(Number(plan.tp_pct) * 100) : "미사용")}${kv("손절", plan.sl_enabled ? fmtPct(Number(plan.sl_pct) * 100) : "미사용")}${kv("추적", plan.trailing_enabled ? fmtPct(Number(plan.trail_pct) * 100) : "미사용")}${kv("타임아웃", fmtDateTime(plan.timeout_ts_ms))}</div>`,
          "mini-card"
        )).join("")
        : empty("활성 리스크 플랜이 없습니다.");

      const intentSection = intents.length
        ? intents.slice(0, 6).map((intent) => card(
          `${intent.market || "-"} · ${translate(intent.side)} · ${translate(intent.status)}`,
          `<div class="kv-grid">${kv("사유", translate(intent.reason_code))}${kv("예상 금액", fmtMoney(intent.notional_quote))}${kv("모델 점수", fmtNumber(intent.prob, 4))}${kv("건너뜀", translate(intent.skip_reason))}${kv("예상 순엣지", fmtBps(intent.expected_net_edge_bps))}${kv("생성 시각", fmtDateTime(intent.ts_ms))}</div>`,
          "mini-card"
        )).join("")
        : empty("최근 intent가 없습니다.");

      return `<article class="live-card ${statePriority(liveState) > 0 ? "priority" : ""}"><div class="row"><h4>${esc(liveState.label)}</h4>${pill("브레이커", boolLabel(liveState.breaker_active), liveState.breaker_active ? "bad" : "good")}</div><p>${esc(summaryLine)}</p><div class="metric-grid">${metric("현재 모델", shortRun(runtime.live_runtime_model_run_id))}${metric("챔피언 포인터", shortRun(runtime.champion_pointer_run_id))}${metric("보유 포지션", maybe(liveState.positions_count, "0"))}${metric("열린 주문", maybe(liveState.open_orders_count, "0"))}${metric("리스크 플랜", maybe(liveState.active_risk_plans_count, "0"))}${metric("주문 허용", boolLabel(rollout.order_emission_allowed))}</div><div class="kv-grid">${kv("운용 모드", translate(rollout.mode))}${kv("포인터 동기화", runtime.model_pointer_divergence ? "어긋남" : "정상")}${kv("WS 신선도", runtime.ws_public_stale ? "오래됨" : "정상")}${kv("마지막 resume", fmtDateTime((liveState.last_resume || {}).generated_at || (liveState.last_resume || {}).completed_at))}${kv("상태 DB", shortPath(liveState.db_path))}${kv("브레이커 사유", activeBreakers.join(" / ") || "없음")}</div><div class="live-sections"><section class="section-block"><h5>보유 종목</h5><div class="stack">${positionSection}</div></section><section class="section-block"><h5>미체결 주문</h5><div class="stack">${orderSection}</div></section><section class="section-block"><h5>매도 전략</h5><div class="stack">${planSection}</div></section><section class="section-block"><h5>최근 의도</h5><div class="stack">${intentSection}</div></section></div></article>`;
    }).join("") || empty("라이브 상태 DB를 찾지 못했습니다.");
  }

  function renderWs(snapshot) {
    const ws = snapshot.ws_public || {};
    const health = ws.health_snapshot || {};
    const latestRun = ws.runs_summary_latest || {};
    const lastRxTs = Math.max(toNumber(health.updated_at_ms) || 0, toNumber((health.last_rx_ts_ms || {}).trade) || 0, toNumber((health.last_rx_ts_ms || {}).orderbook) || 0);
    document.getElementById("ws-headline").textContent = health.connected ? "WS 수집기는 정상 연결 상태입니다." : "WS 수집기가 끊겨 있습니다.";
    document.getElementById("ws-subhead").textContent = health.connected ? "라이브와 학습이 같은 데이터 플레인을 공유합니다." : "데이터 신선도를 먼저 확인해야 합니다.";
    document.getElementById("ws-kpis").innerHTML = [
      metric("연결", boolLabel(health.connected)),
      metric("구독 종목", maybe(health.subscribed_markets_count, "-")),
      metric("최근 수신", fmtAge(lastRxTs)),
      metric("현재 run", shortRun(health.run_id || latestRun.run_id))
    ].join("");
    document.getElementById("ws-details").innerHTML = [
      card("수집기 상태", `<div class="kv-grid">${kv("연결", boolLabel(health.connected))}${kv("재연결 횟수", maybe(health.reconnect_count, "0"))}${kv("최근 수신", fmtDateTime(lastRxTs))}${kv("fatal reason", maybe(health.fatal_reason))}</div>`),
      card("누적 적재", `<div class="kv-grid">${kv("총 적재 행", fmtNumber((health.written_rows || {}).total, 0))}${kv("trade 행", fmtNumber((health.written_rows || {}).trade, 0))}${kv("orderbook 행", fmtNumber((health.written_rows || {}).orderbook, 0))}${kv("총 drop 행", fmtNumber((health.dropped_rows || {}).total, 0))}${kv("최근 run", `parts ${fmtNumber(latestRun.parts, 0)} · rows ${fmtNumber(latestRun.rows_total, 0)}`)}</div>`)
    ].join("");
  }

  function renderAll(snapshot) {
    renderMeta(snapshot);
    renderOverview(snapshot);
    renderTraining(snapshot);
    renderPaper(snapshot);
    renderLive(snapshot);
    renderWs(snapshot);
  }

  async function refresh() {
    try {
      const response = await fetch("/api/snapshot", { cache: "no-store" });
      if (!response.ok) throw new Error(`snapshot 응답 실패 (${response.status})`);
      renderAll(await response.json());
      setError("");
    } catch (err) {
      setError(`실시간 새로고침 실패: ${err && err.message ? err.message : err}`);
    }
  }

  document.getElementById("refresh-btn").addEventListener("click", refresh);
  bindTabs();
  setTab("overview");
  renderAll(INITIAL_SNAPSHOT);
  refresh();
  setInterval(refresh, 10000);
})();
