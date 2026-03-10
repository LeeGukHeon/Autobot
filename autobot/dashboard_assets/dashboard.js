(function () {
  const REASON_TEXT = {
    BACKTEST_ACCEPTANCE_FAILED: "백테스트 승급 기준을 넘지 못했습니다.",
    TRAINER_EVIDENCE_REQUIRED_FAILED: "학습 증거 기준에서 후보 우위가 확정되지 않았습니다.",
    PAPER_SOAK_SKIPPED: "내부 paper soak는 생략했고, 실제 챌린저 페이퍼가 대신 검증합니다.",
    OFFLINE_NOT_CANDIDATE_EDGE: "오프라인 비교에서 후보 우위가 확인되지 않았습니다.",
    SPA_LIKE_NOT_CANDIDATE_EDGE: "공통 구간 SPA 유사 검정에서 후보 우위가 확인되지 않았습니다.",
    WHITE_RC_NOT_CANDIDATE_EDGE: "White Reality Check에서 후보 우위가 확인되지 않았습니다.",
    HANSEN_SPA_NOT_CANDIDATE_EDGE: "Hansen SPA에서 후보 우위가 확인되지 않았습니다.",
    EXECUTION_NOT_CANDIDATE_EDGE: "실행 비용까지 반영하면 챔피언이 더 안정적이었습니다.",
    DUPLICATE_CANDIDATE: "기존 챔피언과 사실상 같은 모델이라 새 챌린저로 올리지 않았습니다.",
    LIVE_BREAKER_ACTIVE: "라이브 브레이커가 활성이라 새 주문을 막고 있습니다.",
    MODEL_POINTER_DIVERGENCE: "라이브가 물고 있는 모델과 현재 챔피언 포인터가 어긋났습니다.",
    WS_PUBLIC_STALE: "공용 WS 수집 신선도가 기준보다 오래됐습니다.",
    UNKNOWN_POSITIONS_DETECTED: "거래소 포지션과 로컬 상태가 달라 브레이커가 동작했습니다.",
    SMALL_ACCOUNT_CANARY_MULTIPLE_ACTIVE_MARKETS: "카나리아 단일 슬롯 제한을 넘는 시장이 감지됐습니다.",
    EXTERNAL_OPEN_ORDERS_DETECTED: "봇이 만들지 않은 외부 미체결 주문이 감지됐습니다.",
    LOCAL_POSITION_MISSING_ON_EXCHANGE: "로컬 포지션은 있는데 거래소 잔고가 사라졌습니다.",
    SKIPPED_SINGLE_SLOT_ACTIVE_ORDER: "이미 열린 주문이 있어 카나리아 단일 슬롯 때문에 건너뛰었습니다.",
    REJECT_EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST: "비용 차감 후 기대 우위가 0 이하라 주문하지 않았습니다.",
    INSUFFICIENT_FREE_BALANCE: "가용 잔고가 부족해서 주문하지 않았습니다.",
    FEE_RESERVE_INSUFFICIENT: "수수료 여유분을 포함하면 잔고가 부족합니다.",
    DUST_REMAINDER: "주문 후 남는 잔량이 너무 작아서 주문하지 않았습니다.",
    BELOW_MIN_TOTAL: "거래소 최소 주문 금액보다 작아서 주문하지 않았습니다.",
    INVALID_PARAMETER: "주문 파라미터가 거래소 규격과 맞지 않아 거절됐습니다."
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
    shadow: "섀도",
    live: "정식 라이브",
    rank_effective_quantile: "순위 기반 컷오프",
    raw_threshold: "절대 점수 컷오프",
    hold: "시간 기반 보유",
    risk: "TP·SL·추적 기반",
    none: "없음",
    ACTIVE: "진행 중",
    TRIGGERED: "매도 대기",
    EXITING: "매도 주문 진행 중",
    CLOSED: "종료 완료",
    wait: "대기",
    done: "완료",
    cancel: "취소",
    bid: "매수",
    ask: "매도",
    limit: "지정가",
    price: "시장가(금액 지정)",
    market: "시장가",
    MODEL_ALPHA_ENTRY_V1: "모델 진입 신호",
    MODEL_ALPHA_EXIT_TIMEOUT: "보유 시간 종료",
    MODEL_ALPHA_EXIT_TP: "익절 조건 충족",
    MODEL_ALPHA_EXIT_SL: "손절 조건 충족",
    MODEL_ALPHA_EXIT_TRAILING: "추적 매도 조건 충족"
  };

  const SERVICE_LABELS = {
    paper_champion: "챔피언 페이퍼",
    paper_challenger: "챌린저 페이퍼",
    ws_public: "공용 WS 수집기",
    live_main: "메인 라이브",
    live_candidate: "후보 카나리아",
    spawn_service: "spawn 서비스",
    promote_service: "promote 서비스",
    spawn_timer: "spawn 타이머",
    promote_timer: "promote 타이머"
  };

  const INITIAL_SNAPSHOT = JSON.parse(document.getElementById("initial-snapshot").textContent || "{}");
  const state = { activeTab: "overview" };

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
    return text.length > 72 ? `…${text.slice(-72)}` : text;
  }

  function coerceTs(value) {
    const asNum = toNumber(value);
    if (asNum == null) return null;
    return asNum > 10000000000 ? asNum : asNum * 1000;
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
    if (sec < 60) return `${sec.toFixed(1)}초 전`;
    if (sec < 3600) return `${(sec / 60).toFixed(1)}분 전`;
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
    state.activeTab = nextTab;
    document.querySelectorAll(".tab-button").forEach((button) => {
      button.classList.toggle("active", button.dataset.tab === nextTab);
    });
    document.querySelectorAll(".pane").forEach((pane) => {
      const active = pane.dataset.pane === nextTab;
      pane.hidden = !active;
    });
  }

  function bindTabs() {
    document.getElementById("tab-bar").addEventListener("click", (event) => {
      const button = event.target.closest(".tab-button");
      if (!button) return;
      setTab(button.dataset.tab);
      window.scrollTo({ top: 0, behavior: "smooth" });
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
    const candidateLive = liveStates.find((item) => String(item.label || "").includes("후보")) || liveStates[0] || {};
    const mainLive = liveStates.find((item) => String(item.label || "").includes("메인")) || {};
    const challenger = snapshot.challenger || {};
    const overallPass = acceptance.overall_pass;

    document.getElementById("overview-headline").textContent =
      overallPass === true ? "이번 후보는 승급 후보로 통과했습니다." :
      overallPass === false ? "이번 후보는 검증에서 탈락했고 챔피언을 유지합니다." :
      "최신 운영 상태를 아직 기다리는 중입니다.";

    document.getElementById("overview-subhead").textContent =
      Number(candidateLive.positions_count || 0) > 0
        ? `후보 카나리아는 현재 ${candidateLive.positions_count}개 포지션을 보유 중이며, 메인 라이브와 분리된 DB로 동작합니다.`
        : challenger.started
          ? "챌린저 생성 절차는 실행됐지만 아직 활성 페이퍼 또는 라이브 포지션은 없습니다."
          : "현재는 챔피언 페이퍼와 라이브가 유지되고 있으며, 새 후보는 최근 검증에서 통과하지 못했습니다.";

    document.getElementById("overview-kpis").innerHTML = [
      metric("후보 run", shortRun(acceptance.candidate_run_id)),
      metric("챔피언 run", shortRun(acceptance.champion_before_run_id || acceptance.champion_after_run_id)),
      metric("후보 라이브 포지션", maybe(candidateLive.positions_count, "0")),
      metric("후보 리스크 플랜", maybe(candidateLive.active_risk_plans_count, "0")),
      metric("메인 라이브 포지션", maybe(mainLive.positions_count, "0")),
      metric("브레이커", boolLabel(Boolean(candidateLive.breaker_active || mainLive.breaker_active)))
    ].join("");

    const services = snapshot.services || {};
    document.getElementById("services-grid").innerHTML = Object.entries(services).map(([key, svc]) => {
      const label = SERVICE_LABELS[key] || key;
      const active = String(svc.active_state || "").toLowerCase();
      const sub = String(svc.sub_state || "").toLowerCase();
      const kind = active === "active" ? "good" : active === "failed" ? "bad" : "warn";
      return `<article class="service-card"><div class="row"><h4>${esc(label)}</h4>${pill("상태", `${translate(active)} / ${translate(sub)}`, kind)}</div><div class="kv-grid">${kv("최근 시작", fmtDateTime(svc.started_at))}${kv("다음 실행", fmtDateTime(svc.next_run_at))}${kv("설명", shortPath(svc.description || svc.unit_file || svc.exec_start || "-"))}</div></article>`;
    }).join("") || empty("표시할 서비스 상태가 없습니다.");

    const notes = [];
    if ((acceptance.reasons || []).length) notes.push(noteCard("이번 후보 직접 사유", unique(acceptance.reasons).map(translate).join(" / "), "warn"));
    if ((acceptance.trainer_reasons || []).length) notes.push(noteCard("학습 증거 세부 사유", unique(acceptance.trainer_reasons).map(translate).join(" / "), "warn"));
    if (challenger.reason) notes.push(noteCard("챌린저 미기동 사유", translate(challenger.reason), "warn"));
    liveStates.forEach((liveState) => {
      const reasons = unique((liveState.active_breakers || []).map((item) => item.reason || item.code || item.name));
      if (reasons.length) notes.push(noteCard(`${liveState.label} 브레이커`, reasons.map(translate).join(" / "), "bad"));
    });
    if (!notes.length) notes.push(noteCard("현재 해석", "치명 브레이커나 즉시 확인이 필요한 운영 경고는 없습니다."));
    document.getElementById("alerts-grid").innerHTML = notes.join("");
  }

  function renderTraining(snapshot) {
    const acceptance = (snapshot.training || {}).acceptance || {};
    const training = snapshot.training || {};
    const challenger = snapshot.challenger || {};
    document.getElementById("training-headline").textContent =
      acceptance.overall_pass === true ? "이번 후보는 학습·검증을 통과했습니다." :
      acceptance.overall_pass === false ? "이번 후보는 검증에서 탈락했습니다." :
      "최신 어셉턴스 결과가 아직 없습니다.";
    document.getElementById("training-subhead").textContent =
      acceptance.overall_pass === false
        ? unique([...(acceptance.reasons || []), ...(acceptance.trainer_reasons || [])]).map(translate).join(" / ") || "직접 사유를 읽는 중입니다."
        : "후보 run, 챔피언 비교, selection policy, calibration, runtime recommendation을 같이 요약합니다.";

    document.getElementById("training-kpis").innerHTML = [
      metric("후보 run", shortRun(acceptance.candidate_run_id)),
      metric("배치 날짜", maybe(acceptance.batch_date)),
      metric("판정 기준", translate(acceptance.decision_basis)),
      metric("최종 완료", fmtDateTime(acceptance.completed_at || acceptance.generated_at))
    ].join("");

    document.getElementById("training-details").innerHTML = [
      card("어셉턴스 요약", `<p>${esc(document.getElementById("training-subhead").textContent)}</p><div class="kv-grid">${kv("모델 패밀리", maybe(acceptance.model_family))}${kv("후보 디렉터리", shortPath(acceptance.candidate_run_dir))}${kv("직전 챔피언", shortRun(acceptance.champion_before_run_id))}${kv("현재 챔피언", shortRun(acceptance.champion_after_run_id))}${kv("백테스트 통과", boolLabel(acceptance.backtest_pass))}${kv("paper 통과", boolLabel(acceptance.paper_pass))}</div>`),
      card("챌린저 루프", `<p>${esc(challenger.started ? "이번 spawn에서 챌린저 기동까지 갔습니다." : "이번 spawn에서는 챌린저를 띄우지 못했습니다.")}</p><div class="kv-grid">${kv("직접 사유", translate(challenger.reason))}${kv("acceptance 메모", unique(challenger.acceptance_notes || []).map(translate).join(" / ") || "-")}${kv("챌린저 유닛", maybe(challenger.challenger_unit))}${kv("리포트 경로", shortPath(challenger.artifact_path))}</div>`)
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
      `<article class="artifact-card"><h4>런타임 추천</h4><p>이번 후보가 실제 운용 시 어떤 종료 모드와 보유 시간을 쓰는지 보여줍니다.</p><div class="kv-grid">${kv("추천 종료 모드", translate(runtime.recommended_exit_mode))}${kv("권장 보유 bar", maybe(runtime.recommended_hold_bars))}${kv("TP / SL / 추적", `${fmtPct((toNumber(runtime.tp_pct) || 0) * 100)} / ${fmtPct((toNumber(runtime.sl_pct) || 0) * 100)} / ${fmtPct((toNumber(runtime.trailing_pct) || 0) * 100)}`)}${kv("추천 산출 방식", maybe(runtime.recommendation_source))}</div></article>`,
      `<article class="artifact-card"><h4>선택 정책 / 보정</h4><p>paper와 live가 공유하는 selection policy와 보정 artifact입니다.</p><div class="kv-grid">${kv("정책 모드", translate(policy.mode))}${kv("기준 키", maybe(policy.threshold_key))}${kv("상위 비율", policy.rank_quantile == null ? "-" : fmtPct(Number(policy.rank_quantile) * 100))}${kv("보정 사용", boolLabel(policy.calibration_enabled))}${kv("보정 방법", maybe(calibration.method))}${kv("보정 샘플 수", maybe(calibration.sample_count))}</div></article>`,
      `<article class="artifact-card"><h4>탐색 예산 / 팩터 선택</h4><p>이번 run에서 search budget과 factor selector가 어떤 결정을 내렸는지 요약합니다.</p><div class="kv-grid">${kv("예산 결정", maybe(budget.decision_mode))}${kv("booster sweep", maybe(budget.booster_sweep_trials))}${kv("runtime grid", maybe(budget.runtime_grid_mode))}${kv("예산 사유", unique(budget.reasons || []).map(translate).join(" / ") || "-")}${kv("허용 블록", (factor.accepted_blocks || []).join(", ") || "-")}${kv("제외 블록", (factor.rejected_blocks || []).join(", ") || "-")}</div></article>`,
      `<article class="artifact-card"><h4>강건성 검증</h4><p>White/Hansen comparable 여부와 CPCV-lite 실행 여부를 같이 보여줍니다.</p><div class="kv-grid">${kv("CPCV 요청", boolLabel(cpcv.requested))}${kv("White comparable", boolLabel(wf.white_rc_comparable))}${kv("Hansen comparable", boolLabel(wf.hansen_spa_comparable))}${kv("selection trial 수", maybe(wf.selection_search_trial_count))}</div></article>`
    ].join("");
  }

  function renderPaper(snapshot) {
    const rows = (snapshot.paper || {}).recent_runs || [];
    document.getElementById("paper-grid").innerHTML = rows.map((run) => {
      const fillRate = run.fill_rate == null ? "-" : fmtPct(Number(run.fill_rate) * 100);
      return `<article class="paper-card"><div class="row"><h4>${esc(shortRun(run.run_id))}</h4>${pill("워밍업", boolLabel(run.warmup_satisfied), run.warmup_satisfied ? "good" : "warn")}</div><p>${esc(`${maybe(run.feature_provider)} / ${maybe(run.micro_provider)} 조합으로 ${fmtNumber(run.duration_sec, 0)}초 동안 돌았습니다.`)}</p><div class="metric-grid">${metric("제출 주문", maybe(run.orders_submitted, "0"))}${metric("체결 주문", maybe(run.orders_filled, "0"))}${metric("체결률", fillRate)}${metric("실현 손익", fmtMoney(run.realized_pnl_quote))}${metric("평가 손익", fmtMoney(run.unrealized_pnl_quote))}${metric("최대 낙폭", fmtPct(run.max_drawdown_pct))}</div><div class="subtle">마지막 갱신: ${esc(fmtDateTime(run.updated_at))}</div></article>`;
    }).join("") || empty("최근 페이퍼 런 요약이 없습니다.");
  }

  function statePriority(item) {
    return (Number(item.positions_count || 0) * 100) +
      (Number(item.open_orders_count || 0) * 50) +
      (Number(item.active_risk_plans_count || 0) * 40) +
      (Number(item.intents_count || 0));
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
      const breakerReasons = unique((liveState.active_breakers || []).map((item) => item.reason || item.code || item.name)).map(translate);
      const priorityClass = statePriority(liveState) > 0 ? "priority" : "";

      const positionSection = positions.length
        ? positions.map((position) => card(
          position.market || "-",
          `<p>보유 수량 ${fmtNumber(position.base_amount, 8)}개 · 평균 매수가 ${fmtMoney(position.avg_entry_price)} · 평가 원금 약 ${fmtMoney((toNumber(position.base_amount) || 0) * (toNumber(position.avg_entry_price) || 0))}</p><div class="kv-grid">${kv("관리 대상", boolLabel(position.managed))}${kv("최근 갱신", fmtDateTime(position.updated_ts))}</div>`,
          "mini-card"
        )).join("")
        : empty("현재 보유 포지션이 없습니다.");

      const orderSection = openOrders.length
        ? openOrders.map((order) => card(
          `${order.market || "-"} · ${translate(order.side)} ${translate(order.ord_type)}`,
          `<p>요청 ${fmtNumber(order.volume_req, 8)}개 · 지정가 ${fmtMoney(order.price)} · 주문금액 약 ${fmtMoney((toNumber(order.volume_req) || 0) * (toNumber(order.price) || 0))}</p><div class="kv-grid">${kv("거래소 상태", translate(order.raw_exchange_state))}${kv("로컬 상태", translate(order.local_state))}${kv("체결 수량", fmtNumber(order.volume_filled, 8))}${kv("replace 횟수", maybe(order.replace_seq, "0"))}${kv("intent", shortRun(order.intent_id))}${kv("최근 갱신", fmtDateTime(order.updated_ts))}</div>`,
          "mini-card"
        )).join("")
        : empty("현재 열린 주문이 없습니다.");

      const planSection = riskPlans.length
        ? riskPlans.map((plan) => card(
          `${plan.market || "-"} · ${translate(plan.exit_mode)}`,
          `<p>플랜 상태 ${translate(plan.state)} · 진입가 ${fmtMoney(plan.entry_price)} · 수량 ${fmtNumber(plan.qty, 8)}개</p><div class="kv-grid">${kv("플랜 source", maybe(plan.plan_source))}${kv("intent", shortRun(plan.source_intent_id))}${kv("익절", plan.tp_enabled ? fmtPct(Number(plan.tp_pct) * 100) : "미사용")}${kv("손절", plan.sl_enabled ? fmtPct(Number(plan.sl_pct) * 100) : "미사용")}${kv("추적", plan.trailing_enabled ? fmtPct(Number(plan.trail_pct) * 100) : "미사용")}${kv("타임아웃", fmtDateTime(plan.timeout_ts_ms))}</div>`,
          "mini-card"
        )).join("")
        : empty("현재 활성 리스크 플랜이 없습니다.");

      const intentSection = intents.length
        ? intents.map((intent) => card(
          `${intent.market || "-"} · ${translate(intent.side)} · ${translate(intent.status)}`,
          `<p>${translate(intent.reason_code)} · 진입 금액 ${fmtMoney(intent.notional_quote)} · 모델 점수 ${fmtNumber(intent.prob, 4)}</p><div class="kv-grid">${kv("선택 정책", translate(intent.selection_policy_mode))}${kv("예상 순엣지", fmtBps(intent.expected_net_edge_bps))}${kv("예상 비용", fmtBps(intent.estimated_total_cost_bps))}${kv("건너뜀 사유", translate(intent.skip_reason))}${kv("요청가", fmtMoney(intent.price))}${kv("생성 시각", fmtDateTime(intent.ts_ms))}</div>`,
          "mini-card"
        )).join("")
        : empty("최근 기록된 intent가 없습니다.");

      return `<article class="live-card ${priorityClass}"><div class="row"><h4>${esc(liveState.label)}</h4>${pill("브레이커", boolLabel(liveState.breaker_active), liveState.breaker_active ? "bad" : "good")}</div><p>${esc(`${liveState.label}는 현재 ${translate(rollout.mode)} 모드이며, 현재 모델 ${shortRun(runtime.live_runtime_model_run_id)}와 챔피언 포인터 ${shortRun(runtime.champion_pointer_run_id)}를 비교해 동작합니다.`)}</p><div class="metric-grid">${metric("보유 포지션", maybe(liveState.positions_count, "0"))}${metric("열린 주문", maybe(liveState.open_orders_count, "0"))}${metric("활성 리스크 플랜", maybe(liveState.active_risk_plans_count, "0"))}${metric("최근 intent", maybe(liveState.intents_count, "0"))}${metric("주문 방출 허용", boolLabel(rollout.order_emission_allowed))}${metric("WS 신선도", runtime.ws_public_stale ? "오래됨" : "정상")}</div><div class="kv-grid">${kv("현재 모델", shortRun(runtime.live_runtime_model_run_id))}${kv("챔피언 포인터", shortRun(runtime.champion_pointer_run_id))}${kv("포인터 동기화", runtime.model_pointer_divergence ? "어긋남" : "정상")}${kv("마지막 resume", fmtDateTime((liveState.last_resume || {}).generated_at || (liveState.last_resume || {}).completed_at))}${kv("상태 DB", shortPath(liveState.db_path))}${kv("브레이커 사유", breakerReasons.join(" / ") || "없음")}</div><div class="live-sections"><section class="section-block"><h5>보유 종목</h5><p class="section-copy">현재 실제로 들고 있는 종목과 평균 매수가입니다.</p><div class="stack">${positionSection}</div></section><section class="section-block"><h5>미체결 주문</h5><p class="section-copy">아직 체결되지 않은 주문과 그 주문 금액, 상태입니다.</p><div class="stack">${orderSection}</div></section><section class="section-block"><h5>활성 리스크 플랜</h5><p class="section-copy">선택된 매도 전략, 타임아웃, TP·SL·추적 설정입니다.</p><div class="stack">${planSection}</div></section><section class="section-block"><h5>최근 진입 / 종료 의도</h5><p class="section-copy">전략이 최근에 무엇을 왜 사고팔려 했는지 기록입니다.</p><div class="stack">${intentSection}</div></section></div></article>`;
    }).join("") || empty("라이브 상태 DB를 찾지 못했습니다.");
  }

  function renderWs(snapshot) {
    const ws = snapshot.ws_public || {};
    const health = ws.health_snapshot || {};
    const latestRun = ws.runs_summary_latest || {};
    const lastRxTs = Math.max(toNumber(health.updated_at_ms) || 0, toNumber((health.last_rx_ts_ms || {}).trade) || 0, toNumber((health.last_rx_ts_ms || {}).orderbook) || 0);
    document.getElementById("ws-headline").textContent = health.connected ? "공용 WS 수집기는 현재 정상 연결 상태입니다." : "공용 WS 수집기가 현재 끊겨 있습니다.";
    document.getElementById("ws-subhead").textContent = health.connected
      ? "라이브와 학습이 같은 공용 데이터 플레인을 공유합니다."
      : "원천 데이터 신선도가 오래되면 라이브와 학습이 모두 영향을 받습니다.";
    document.getElementById("ws-kpis").innerHTML = [
      metric("연결 상태", boolLabel(health.connected)),
      metric("구독 종목 수", maybe(health.subscribed_markets_count, "-")),
      metric("최근 수신", fmtAge(lastRxTs)),
      metric("현재 run", shortRun(health.run_id || latestRun.run_id))
    ].join("");
    document.getElementById("ws-details").innerHTML = [
      card("수집기 연결", `<p>${esc(document.getElementById("ws-subhead").textContent)}</p><div class="kv-grid">${kv("연결", boolLabel(health.connected))}${kv("fatal reason", maybe(health.fatal_reason))}${kv("재연결 횟수", maybe(health.reconnect_count, "0"))}${kv("최근 수신 시각", fmtDateTime(lastRxTs))}</div>`),
      card("누적 적재 상태", `<div class="kv-grid">${kv("총 적재 행", fmtNumber((health.written_rows || {}).total, 0))}${kv("trade 적재 행", fmtNumber((health.written_rows || {}).trade, 0))}${kv("orderbook 적재 행", fmtNumber((health.written_rows || {}).orderbook, 0))}${kv("총 drop 행", fmtNumber((health.dropped_rows || {}).total, 0))}${kv("downsample drop", fmtNumber((health.dropped_rows || {}).orderbook_downsample, 0))}${kv("최근 run 요약", `parts ${fmtNumber(latestRun.parts, 0)} · rows ${fmtNumber(latestRun.rows_total, 0)}`)}</div>`)
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
