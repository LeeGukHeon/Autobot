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
    CANARY_SLOT_UNAVAILABLE: "카나리아 슬롯 사용 중",
    MODEL_ALPHA_EXIT_HOLD_TIMEOUT: "보유 시간이 끝나 자동으로 정리",
    MODEL_ALPHA_EXIT_TIMEOUT: "보유 시간이 끝나 정리",
    MODEL_ALPHA_EXIT_TP: "목표 수익에 도달해 정리",
    MODEL_ALPHA_EXIT_SL: "허용 손실을 넘어 정리",
    MODEL_ALPHA_EXIT_TRAILING: "수익 보호 추적선에 닿아 정리",
    POLICY_OK: "정책 통과",
    ALLOW: "진입 가능"
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
    MODEL_ALPHA_EXIT_TRAILING: "추적 매도",
    SUBMITTED: "제출됨",
    SKIPPED: "건너뜀"
  };

  const SERVICE_LABELS = {
    paper_champion: "페이퍼 챔피언",
    paper_challenger: "페이퍼 챌린저",
    ws_public: "WS 수집기",
    live_main: "메인 라이브",
    live_candidate: "후보 카나리아",
    spawn_service: "챌린저 생성 서비스",
    promote_service: "챌린저 승급 서비스",
    rank_shadow_service: "랭크 그림자 서비스",
    spawn_timer: "챌린저 생성 타이머",
    promote_timer: "챌린저 승급 타이머",
    rank_shadow_timer: "랭크 그림자 타이머"
  };

  const TABS = new Set(["overview", "training", "paper", "live", "ws"]);
  const INITIAL_SNAPSHOT = JSON.parse(document.getElementById("initial-snapshot").textContent || "{}");
  const state = {
    activeTab: TABS.has(location.hash.replace("#", "")) ? location.hash.replace("#", "") : "overview",
    activeLiveLabel: null
  };

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
    return num == null ? "-" : `${num.toLocaleString("ko-KR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    })}%`;
  }

  function fmtBps(value) {
    const num = toNumber(value);
    return num == null ? "-" : `${num.toLocaleString("ko-KR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    })}bps`;
  }

  function shortRun(value) {
    const text = String(value || "").trim();
    if (!text) return "-";
    return text.length > 18 ? `${text.slice(0, 18)}…` : text;
  }

  function shortPath(value) {
    const text = String(value || "").trim();
    if (!text) return "-";
    return text.length > 54 ? `…${text.slice(-54)}` : text;
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

  function joinTranslated(values) {
    return unique(values).map(translate).join(" / ") || "-";
  }

  function yesNoSentence(value, yesText, noText, unknownText = "-") {
    if (value === true) return yesText;
    if (value === false) return noText;
    return unknownText;
  }

  function tradeActionSummary(intent) {
    const action = intent.trade_action_recommended_action;
    const edge = fmtBps(intent.trade_action_expected_edge_bps);
    const downside = fmtBps(intent.trade_action_expected_downside_bps);
    const multiple = fmtNumber(intent.trade_action_notional_multiplier, 2);
    if (!action || action === "-") return "학습된 trade action 정보가 아직 없습니다.";
    return `${translate(action)} 전략으로 판단했고, 기대 순엣지는 ${edge}, 예상 하방 변동은 ${downside}, 진입 금액 배수는 ${multiple}배로 계산됐습니다.`;
  }

  function intentNarrative(intent) {
    const market = intent.market || "이 종목";
    const side = translate(intent.side);
    const status = translate(intent.status);
    if (intent.skip_reason) {
      return `${market} ${side} 주문은 ${translate(intent.skip_reason)} 때문에 보내지지 않았습니다.`;
    }
    if (intent.side === "ask") {
      return `${market} 정리 주문이 접수됐습니다. 이유는 ${translate(intent.reason_code)}입니다.`;
    }
    if (intent.status === "SUBMITTED") {
      return `${market} 진입 주문이 접수됐습니다. 예상 순엣지와 비용 검토를 통과했고, 카나리아 제한도 만족했습니다.`;
    }
    return `${market} ${side} 의도는 현재 ${status} 상태입니다.`;
  }

  function riskPlanNarrative(plan) {
    if (!plan) return "활성 매도 계획이 없습니다.";
    if (plan.exit_mode === "hold") {
      return `${plan.market}는 시간을 기준으로 관리합니다. ${holdModeText(plan)} 기준으로 자동 정리됩니다.`;
    }
    return `${plan.market}는 손절/익절/추적 매도로 관리합니다. ${riskModeText(plan)} 기준으로 자동 정리됩니다.`;
  }

  function runtimeExplain(runtime) {
    const action = runtime.trade_action || {};
    const bins = action.sample_bins || [];
    const firstBin = bins[0] || {};
    const actionText = action.status === "ready"
      ? `trade action이 활성화되어 있고, ${translate(firstBin.recommended_action)} 쪽 bin 예시가 ${bins.length}개 보입니다.`
      : "trade action은 아직 비활성 또는 준비 전입니다.";
    return `${translate(runtime.recommended_exit_mode)} 모드를 기본 청산 추천으로 쓰며, ${actionText}`;
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

  function setTab(nextTab, updateHash = true) {
    if (!TABS.has(nextTab)) return;
    state.activeTab = nextTab;
    document.querySelectorAll(".tab-button").forEach((button) => {
      const active = button.dataset.tab === nextTab;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.querySelectorAll(".pane").forEach((pane) => {
      pane.hidden = pane.dataset.pane !== nextTab;
    });
    if (updateHash) history.replaceState(null, "", `#${nextTab}`);
  }

  function bindTabs() {
    document.getElementById("tab-bar").addEventListener("click", (event) => {
      const button = event.target.closest(".tab-button");
      if (!button) return;
      event.preventDefault();
      setTab(button.dataset.tab);
    });
    window.addEventListener("hashchange", () => {
      const nextTab = location.hash.replace("#", "");
      if (TABS.has(nextTab)) setTab(nextTab, false);
    });
  }

  function setLiveLabel(label) {
    state.activeLiveLabel = label || null;
    document.querySelectorAll(".subtab-button").forEach((button) => {
      button.classList.toggle("active", button.dataset.liveLabel === state.activeLiveLabel);
    });
  }

  function renderMeta(snapshot) {
    document.getElementById("generated-at").textContent = fmtDateTime(snapshot.generated_at);
    document.getElementById("project-root").textContent = snapshot.project_root || "-";
    const system = snapshot.system || {};
    const projectGb = Number(system.project_used_bytes || 0) / (1024 ** 3);
    const fsUsedGb = Number(system.used_bytes || 0) / (1024 ** 3);
    const totalGb = Number(system.total_bytes || 0) / (1024 ** 3);
    document.getElementById("storage-summary").textContent =
      `프로젝트 ${fmtNumber(projectGb, 1)} GB · 전체 ${fmtNumber(fsUsedGb, 1)} / ${fmtNumber(totalGb, 1)} GB`;
  }

  function renderOverview(snapshot) {
    const acceptance = (snapshot.training || {}).acceptance || {};
    const liveStates = (snapshot.live || {}).states || [];
    const candidateLive = liveStates.find((item) => String(item.label || "").includes("후보")) || {};
    const challenger = snapshot.challenger || {};

    document.getElementById("overview-headline").textContent =
      acceptance.overall_pass === true ? "이번 후보는 통과했습니다." :
      acceptance.overall_pass === false ? "이번 후보는 탈락했고 챔피언 유지입니다." :
      "최신 검증 결과를 기다리는 중입니다.";

    document.getElementById("overview-subhead").textContent =
      Number(candidateLive.positions_count || 0) > 0
        ? `후보 카나리아는 현재 ${candidateLive.positions_count}개 포지션을 보유 중입니다.`
        : challenger.started
          ? "챌린저 생성 절차는 실행됐지만 아직 활성 포지션은 없습니다."
          : "현재는 챔피언 중심 운영 상태입니다.";

    document.getElementById("overview-kpis").innerHTML = [
      metric("후보 run", shortRun(acceptance.candidate_run_id)),
      metric("판정", acceptance.overall_pass === true ? "통과" : acceptance.overall_pass === false ? "탈락" : "-"),
      metric("후보 포지션", maybe(candidateLive.positions_count, "0")),
      metric("후보 리스크 플랜", maybe(candidateLive.active_risk_plans_count, "0"))
    ].join("");

    const services = snapshot.services || {};
    document.getElementById("services-grid").innerHTML = Object.entries(services).map(([key, svc]) => {
      const active = String(svc.active_state || "").toLowerCase();
      const sub = String(svc.sub_state || "").toLowerCase();
      return `<article class="service-card"><div class="row"><h4>${esc(SERVICE_LABELS[key] || key)}</h4>${pill("상태", `${translate(active)} / ${translate(sub)}`, active === "active" ? "good" : active === "failed" ? "bad" : "warn")}</div><div class="kv-grid">${kv("최근 시작", fmtDateTime(svc.started_at))}${kv("다음 실행", fmtDateTime(svc.next_run_at))}</div></article>`;
    }).join("") || empty("표시할 서비스가 없습니다.");

    const notes = [];
    if ((acceptance.reasons || []).length) notes.push(noteCard("직접 사유", unique(acceptance.reasons).map(translate).join(" / "), "warn"));
    if ((acceptance.trainer_reasons || []).length) notes.push(noteCard("학습 증거", unique(acceptance.trainer_reasons).map(translate).join(" / "), "warn"));
    if (challenger.reason) notes.push(noteCard("챌린저 미기동 사유", translate(challenger.reason), "warn"));
    liveStates.forEach((liveState) => {
      const breakers = unique((liveState.active_breakers || []).map((item) => item.reason || item.code || item.name));
      if (breakers.length) notes.push(noteCard(`${liveState.label} 브레이커`, breakers.map(translate).join(" / "), "bad"));
    });
    document.getElementById("alerts-grid").innerHTML =
      notes.length ? notes.join("") : noteCard("현재 상태", "즉시 확인이 필요한 경고는 없습니다.");
  }

  function renderTraining(snapshot) {
    const acceptance = (snapshot.training || {}).acceptance || {};
    const training = snapshot.training || {};
    const challenger = snapshot.challenger || {};
    const rankShadow = training.rank_shadow || {};
    const summary = joinTranslated([...(acceptance.reasons || []), ...(acceptance.trainer_reasons || [])]);

    document.getElementById("training-headline").textContent =
      acceptance.overall_pass === true ? "최신 후보가 검증을 통과했습니다." :
      acceptance.overall_pass === false ? "최신 후보가 검증을 통과하지 못했습니다." :
      "아직 최근 검증 결과가 없습니다.";
    document.getElementById("training-subhead").textContent = summary;
    document.getElementById("training-kpis").innerHTML = [
      metric("후보 run", shortRun(acceptance.candidate_run_id)),
      metric("배치 날짜", maybe(acceptance.batch_date)),
      metric("판정 기준", translate(acceptance.decision_basis)),
      metric("갱신 시각", fmtDateTime(acceptance.completed_at || acceptance.generated_at))
    ].join("");

    const acceptanceNarrative = acceptance.overall_pass === true
      ? "백테스트와 보조 증거를 기준으로 이번 후보를 다음 단계로 넘길 수 있는 상태입니다."
      : acceptance.overall_pass === false
        ? "이번 후보는 적어도 한 개 이상의 검증 문턱을 넘지 못했습니다."
        : "아직 acceptance 결과가 기록되지 않았습니다.";
    const challengerNarrative = challenger.started
      ? "챌린저 서비스가 실제로 올라가 다음 단계 관찰이 시작됐습니다."
      : `${translate(challenger.reason)} 때문에 챌린저가 아직 올라가지 않았습니다.`;
    const rankNarrative = rankShadow.status
      ? `랭크 그림자 레인은 현재 ${maybe(rankShadow.status)} 상태이며 다음 액션은 ${maybe(rankShadow.next_action)}입니다.`
      : "랭크 그림자 레인 최신 판단이 아직 없습니다.";
    document.getElementById("training-details").innerHTML = [
      card("이번 후보 해석", `<p class="section-copy">${esc(acceptanceNarrative)}</p><div class="kv-grid">${kv("모델 계열", maybe(acceptance.model_family))}${kv("후보 run", shortRun(acceptance.candidate_run_id))}${kv("이전 챔피언", shortRun(acceptance.champion_before_run_id))}${kv("현재 챔피언", shortRun(acceptance.champion_after_run_id))}${kv("백테스트 통과", boolLabel(acceptance.backtest_pass))}${kv("페이퍼 통과", boolLabel(acceptance.paper_pass))}</div>`),
      card("챌린저 루프", `<p class="section-copy">${esc(challengerNarrative)}</p><div class="kv-grid">${kv("챌린저 시작", challenger.started ? "시작됨" : "미시작")}${kv("멈춘 이유", translate(challenger.reason))}${kv("추가 메모", joinTranslated(challenger.acceptance_notes || []))}${kv("보고서", shortPath(challenger.artifact_path))}</div>`),
      card("랭크 그림자 레인", `<p class="section-copy">${esc(rankNarrative)}</p><div class="kv-grid">${kv("현재 상태", maybe(rankShadow.status))}${kv("다음 액션", maybe(rankShadow.next_action))}${kv("선택 레인", maybe((rankShadow.governance_action || {}).selected_lane_id))}${kv("선택 스크립트", maybe((rankShadow.governance_action || {}).selected_acceptance_script))}${kv("후보 run", shortRun(rankShadow.candidate_run_id))}${kv("사이클 보고서", shortPath(rankShadow.artifact_path))}</div>`)
    ].join("");

    const artifacts = training.candidate_artifacts || {};
    const runtime = artifacts.runtime_recommendations || {};
    const policy = artifacts.selection_policy || {};
    const calibration = artifacts.selection_calibration || {};
    const budget = artifacts.search_budget_decision || {};
    const wf = artifacts.walk_forward_report || {};
    const tradeAction = runtime.trade_action || {};
    const tradeActionSample = (tradeAction.sample_bins || [])[0] || {};

    document.getElementById("artifact-grid").innerHTML = [
      `<article class="artifact-card"><h4>실전 주문 추천</h4><p class="section-copy">${esc(runtimeExplain(runtime))}</p><div class="kv-grid">${kv("기본 청산 방식", translate(runtime.recommended_exit_mode))}${kv("기본 보유 바", maybe(runtime.recommended_hold_bars))}${kv("리스크 기준 변동성", maybe(runtime.recommended_risk_vol_feature))}${kv("추천 근거", maybe(runtime.recommendation_source))}</div></article>`,
      `<article class="artifact-card"><h4>진입 선택 규칙</h4><p class="section-copy">후보를 고를 때는 ${translate(policy.mode)} 방식을 쓰고, 점수 보정은 ${maybe(calibration.method)} 기준으로 적용합니다.</p><div class="kv-grid">${kv("선택 방식", translate(policy.mode))}${kv("기준 키", maybe(policy.threshold_key))}${kv("순위 비율", policy.rank_quantile == null ? "-" : fmtPct(Number(policy.rank_quantile) * 100))}${kv("보정 방식", maybe(calibration.method))}</div></article>`,
      `<article class="artifact-card"><h4>Trade Action 정책</h4><p class="section-copy">${tradeAction.status === "ready" ? `학습된 trade action이 활성화돼 있습니다. 현재 예시 bin에서는 ${translate(tradeActionSample.recommended_action)} 전략과 ${fmtBps(tradeActionSample.expected_edge_bps)} 기대 엣지를 사용합니다.` : "trade action 정책이 아직 준비되지 않았습니다."}</p><div class="kv-grid">${kv("정책 상태", maybe(tradeAction.status))}${kv("사용 리스크 변수", maybe(tradeAction.risk_feature_name))}${kv("hold 추천 bin", maybe(tradeAction.hold_bins_recommended))}${kv("risk 추천 bin", maybe(tradeAction.risk_bins_recommended))}${kv("예시 엣지", fmtBps(tradeActionSample.expected_edge_bps))}${kv("예시 진입 배수", fmtNumber(tradeActionSample.notional_multiplier, 2))}</div></article>`,
      `<article class="artifact-card"><h4>검증과 예산</h4><p class="section-copy">이번 후보는 ${maybe(wf.windows_run)}개 검증 구간과 ${maybe(wf.selection_search_trial_count)}개 선택 실험을 바탕으로 평가됐고, 검색 예산은 ${maybe(budget.decision_mode)} 모드로 적용됐습니다.</p><div class="kv-grid">${kv("White 검정 비교 가능", boolLabel(wf.white_rc_comparable))}${kv("Hansen 검정 비교 가능", boolLabel(wf.hansen_spa_comparable))}${kv("선택 실험 수", maybe(wf.selection_search_trial_count))}${kv("예산 모드", maybe(budget.decision_mode))}${kv("부스터 시도 수", maybe(budget.booster_sweep_trials))}${kv("예산 메모", joinTranslated(budget.reasons || []))}</div></article>`
    ].join("");
  }

  function renderPaper(snapshot) {
    const rows = [...((snapshot.paper || {}).recent_runs || [])].sort((a, b) => {
      return (coerceTs(b.updated_at) || 0) - (coerceTs(a.updated_at) || 0);
    });
    document.getElementById("paper-grid").innerHTML = rows.map((run) => {
      return `<article class="paper-card"><div class="row"><h4>${esc(shortRun(run.run_id))}</h4>${pill("워밍업", boolLabel(run.warmup_satisfied), run.warmup_satisfied ? "good" : "warn")}</div><div class="metric-grid">${metric("제출", maybe(run.orders_submitted, "0"))}${metric("체결", maybe(run.orders_filled, "0"))}${metric("체결률", run.fill_rate == null ? "-" : fmtPct(Number(run.fill_rate) * 100))}${metric("실현 손익", fmtMoney(run.realized_pnl_quote))}${metric("평가 손익", fmtMoney(run.unrealized_pnl_quote))}${metric("최대 낙폭", fmtPct(run.max_drawdown_pct))}</div><div class="subtle">${esc(maybe(run.feature_provider))} / ${esc(maybe(run.micro_provider))} · ${esc(fmtDateTime(run.updated_at))}</div></article>`;
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

  function holdModeText(plan) {
    const total = toNumber(plan.hold_total_minutes);
    const elapsed = toNumber(plan.hold_elapsed_minutes);
    const remaining = toNumber(plan.hold_remaining_minutes);
    if (total != null) {
      const parts = [`시간 보유 ${Math.max(0, total)}분`];
      if (elapsed != null) parts.push(`${Math.max(0, elapsed)}분 경과`);
      if (remaining != null) parts.push(`${Math.max(0, remaining)}분 남음`);
      return parts.join(" · ");
    }
    return plan.timeout_ts_ms ? `시간 보유 · ${fmtDateTime(plan.timeout_ts_ms)} 종료 예정` : "시간 보유";
  }

  function riskModeText(plan) {
    const parts = [];
    if (plan.tp_enabled) parts.push(`익절 ${fmtPct(Number(plan.tp_pct) * 100)}`);
    if (plan.sl_enabled) parts.push(`손절 ${fmtPct(Number(plan.sl_pct) * 100)}`);
    if (plan.trailing_enabled) parts.push(`추적 ${fmtPct(Number(plan.trail_pct) * 100)}`);
    if (plan.timeout_ts_ms) parts.push(`최대 보유 ${fmtDateTime(plan.timeout_ts_ms)} 종료`);
    return parts.join(" · ") || "리스크 전략";
  }

  function describeExit(plan, position, order) {
    if (!plan) return "활성 매도 전략이 없습니다.";
    const planText = plan.exit_mode === "hold" ? holdModeText(plan) : riskModeText(plan);
    const orderText = order ? `${fmtMoney(order.price)} 매도 주문 대기` : "아직 매도 주문 없음";
    return `${plan.market} ${fmtNumber(position?.base_amount ?? plan.qty, 8)}개 보유 · 평균 ${fmtMoney(position?.avg_entry_price ?? plan.entry_price)} · ${planText} · ${orderText}`;
  }

  function renderLive(snapshot) {
    const states = [...((snapshot.live || {}).states || [])].sort((a, b) => statePriority(b) - statePriority(a));
    const summaryStrip = document.getElementById("live-summary-strip");
    const tabBar = document.getElementById("live-tab-bar");
    const container = document.getElementById("live-state-list");

    summaryStrip.innerHTML = states.map((liveState) => {
      const highlight = Number(liveState.positions_count || 0) > 0 ? "good" : Number(liveState.open_orders_count || 0) > 0 ? "warn" : "neutral";
      return `<div class="metric"><div class="k">${esc(liveState.label)}</div><div class="v">${esc(`${maybe(liveState.positions_count, "0")}개 보유 / ${maybe(liveState.open_orders_count, "0")}개 주문`)}</div><div style="margin-top:8px">${pill("브레이커", boolLabel(liveState.breaker_active), liveState.breaker_active ? "bad" : highlight)}</div></div>`;
    }).join("") || empty("라이브 상태가 없습니다.");

    if (!state.activeLiveLabel || !states.some((item) => item.label === state.activeLiveLabel)) {
      state.activeLiveLabel = (states[0] || {}).label || null;
    }

    tabBar.innerHTML = states.map((liveState) => {
      const active = liveState.label === state.activeLiveLabel;
      return `<button class="subtab-button ${active ? "active" : ""}" type="button" data-live-label="${esc(liveState.label)}">${esc(liveState.label)}</button>`;
    }).join("");

    tabBar.onclick = (event) => {
      const button = event.target.closest(".subtab-button");
      if (!button) return;
      event.preventDefault();
      setLiveLabel(button.dataset.liveLabel);
      renderLive(snapshot);
    };

    const selected = states.find((item) => item.label === state.activeLiveLabel) || states[0];
    if (!selected) {
      container.innerHTML = empty("라이브 상태 DB를 찾지 못했습니다.");
      return;
    }

    const runtime = selected.runtime_health || {};
    const rollout = selected.rollout_status || {};
    const positions = [...(selected.positions || [])];
    const openOrders = [...(selected.open_orders || [])].sort((a, b) => (coerceTs(b.updated_ts) || 0) - (coerceTs(a.updated_ts) || 0));
    const riskPlans = [...(selected.active_risk_plans || [])].sort((a, b) => (coerceTs(b.updated_ts) || 0) - (coerceTs(a.updated_ts) || 0));
    const intents = [...(selected.recent_intents || [])].sort((a, b) => (coerceTs(b.ts_ms) || 0) - (coerceTs(a.ts_ms) || 0));
    const today = selected.today_trade_summary || {};
    const recentTrades = [...(selected.recent_trades || [])].sort((a, b) => {
      const left = coerceTs(b.exit_ts_ms) || coerceTs(b.entry_ts_ms) || coerceTs(b.updated_ts) || 0;
      const right = coerceTs(a.exit_ts_ms) || coerceTs(a.entry_ts_ms) || coerceTs(a.updated_ts) || 0;
      return left - right;
    });
    const activeBreakers = unique((selected.active_breakers || []).map((item) => item.reason || item.code || item.name)).map(translate);
    const primaryPosition = positions[0];
    const primaryPlan = riskPlans.find((item) => item.market === (primaryPosition || {}).market) || riskPlans[0];
    const primaryOrder = openOrders.find((item) => item.market === (primaryPosition || {}).market && item.side === "ask") || openOrders[0];

    const topSummary = primaryPosition && primaryPlan
      ? describeExit(primaryPlan, primaryPosition, primaryOrder)
      : `${selected.label}는 현재 보유 포지션이 없습니다.`;
    const liveNarrative = positions.length
      ? `현재 ${positions.length}개 포지션을 관리 중이며, 가장 최근 포지션은 ${riskPlanNarrative(primaryPlan)}`
      : `${selected.label}는 현재 비어 있고, 새 진입 신호만 기다리는 상태입니다.`;
    const todaySummaryLine = `${maybe(today.date_label, "-")} ${maybe(today.timezone, "KST")} 기준으로 종료 ${maybe(today.closed_count, "0")}건, 승 ${maybe(today.wins, "0")} / 패 ${maybe(today.losses, "0")} / 보합 ${maybe(today.flats, "0")}, 순손익 ${fmtMoney(today.net_pnl_quote_total)}입니다.`;

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
        `<div class="kv-grid">${kv("주문가", fmtMoney(order.price))}${kv("요청 수량", fmtNumber(order.volume_req, 8))}${kv("체결 수량", fmtNumber(order.volume_filled, 8))}${kv("거래소 상태", translate(order.raw_exchange_state))}${kv("최근 갱신", fmtDateTime(order.updated_ts))}</div>`,
        "mini-card"
      )).join("")
      : empty("미체결 주문이 없습니다.");

    const planSection = riskPlans.length
      ? riskPlans.map((plan) => {
        const exitSummary = plan.exit_mode === "hold" ? holdModeText(plan) : riskModeText(plan);
        return card(
          `${plan.market || "-"} · ${translate(plan.exit_mode)}`,
          `<p class="section-brief">${esc(riskPlanNarrative(plan))}</p><div class="kv-grid">${kv("플랜 상태", translate(plan.state))}${kv("플랜 source", maybe(plan.plan_source))}${kv("연결 intent", shortRun(plan.source_intent_id))}${kv("익절", plan.tp_enabled ? fmtPct(Number(plan.tp_pct) * 100) : "미사용")}${kv("손절", plan.sl_enabled ? fmtPct(Number(plan.sl_pct) * 100) : "미사용")}${kv("추적", plan.trailing_enabled ? fmtPct(Number(plan.trail_pct) * 100) : "미사용")}${kv("종료 시각", fmtDateTime(plan.timeout_ts_ms))}</div>`,
          "mini-card"
        );
      }).join("")
      : empty("활성 리스크 플랜이 없습니다.");

    const intentSection = intents.length
      ? intents.slice(0, 4).map((intent) => card(
        `${intent.market || "-"} · ${translate(intent.side)} · ${translate(intent.status)}`,
        `<p class="section-brief">${esc(intentNarrative(intent))}</p><p class="section-brief">${esc(tradeActionSummary(intent))}</p><div class="kv-grid">${kv("사유", translate(intent.reason_code))}${kv("선택 방식", translate(intent.selection_policy_mode))}${kv("예상 금액", fmtMoney(intent.notional_quote))}${kv("예상 순엣지", fmtBps(intent.expected_net_edge_bps))}${kv("비용 합계", fmtBps(intent.estimated_total_cost_bps))}${kv("Trade 액션", translate(intent.trade_action_recommended_action))}${kv("Trade 엣지", fmtBps(intent.trade_action_expected_edge_bps))}${kv("Trade 하방", fmtBps(intent.trade_action_expected_downside_bps))}${kv("사이징 배수", fmtNumber(intent.trade_action_notional_multiplier, 2))}${kv("생성 시각", fmtDateTime(intent.ts_ms))}${kv("건너뜀", translate(intent.skip_reason))}</div>`,
        "mini-card"
      )).join("")
      : empty("최근 의도가 없습니다.");

    const tradeSection = recentTrades.length
      ? recentTrades.slice(0, 4).map((trade) => {
        const direction = trade.status === "CLOSED" ? "거래 종료" : trade.status === "OPEN" ? "보유 중" : "진입 대기";
        const pnlText = trade.realized_pnl_quote == null
          ? "손익 계산 전"
          : `${fmtMoney(trade.realized_pnl_quote)} / ${fmtPct(trade.realized_pnl_pct)}`;
        const grossText = trade.gross_pnl_quote == null
          ? "계산 전"
          : `${fmtMoney(trade.gross_pnl_quote)} / ${fmtPct(trade.gross_pnl_pct)}`;
        const durationText = trade.hold_minutes == null ? "보유 시간 계산 전" : `${trade.hold_minutes}분 보유`;
        return card(
          `${trade.market || "-"} · ${direction}`,
          `<p class="section-brief">${esc(`${translate(trade.entry_reason_code)}로 진입했고, ${translate(trade.close_mode)} 방식으로 ${translate(trade.close_reason_code)}에 종료됐습니다.`)}</p><div class="kv-grid">${kv("진입 시각", fmtDateTime(trade.entry_ts_ms))}${kv("종료 시각", fmtDateTime(trade.exit_ts_ms))}${kv("보유 시간", durationText)}${kv("진입가", fmtMoney(trade.entry_price))}${kv("종료가", fmtMoney(trade.exit_price))}${kv("수량", fmtNumber(trade.qty, 8))}${kv("순손익", pnlText)}${kv("총손익", grossText)}${kv("총수수료", fmtMoney(trade.total_fee_quote))}${kv("진입 슬리피지", fmtBps(trade.entry_realized_slippage_bps))}${kv("예상 종료 슬리피지", fmtBps(trade.exit_expected_slippage_bps))}${kv("예상 순엣지", fmtBps(trade.expected_net_edge_bps))}${kv("Trade 액션", translate(trade.trade_action))}${kv("Trade 엣지", fmtBps(trade.expected_edge_bps))}${kv("Trade 하방", fmtBps(trade.expected_downside_bps))}${kv("사이징 배수", fmtNumber(trade.notional_multiplier, 2))}</div>`,
          "mini-card"
        );
      }).join("")
      : empty("아직 거래 저널이 없습니다.");

    container.innerHTML = `<article class="live-card priority"><div class="row"><h4>${esc(selected.label)}</h4>${pill("브레이커", boolLabel(selected.breaker_active), selected.breaker_active ? "bad" : "good")}</div><p>${esc(topSummary)}</p><p class="section-copy">${esc(liveNarrative)}</p><div class="metric-grid">${metric("현재 모델", shortRun(runtime.live_runtime_model_run_id))}${metric("챔피언 포인터", shortRun(runtime.champion_pointer_run_id))}${metric("보유 포지션", maybe(selected.positions_count, "0"))}${metric("열린 주문", maybe(selected.open_orders_count, "0"))}${metric("리스크 플랜", maybe(selected.active_risk_plans_count, "0"))}${metric("주문 허용", boolLabel(rollout.order_emission_allowed))}</div><div class="detail-box" style="margin-top:12px"><h4>오늘 거래 요약</h4><p class="section-copy">${esc(todaySummaryLine)}</p><div class="metric-grid">${metric("오늘 종료", maybe(today.closed_count, "0"))}${metric("오늘 승패", `${maybe(today.wins, "0")}승 / ${maybe(today.losses, "0")}패`)}${metric("오늘 순손익", fmtMoney(today.net_pnl_quote_total))}${metric("오늘 수수료", fmtMoney(today.fee_quote_total))}${metric("오늘 미종결", `${maybe(today.open_count, "0")}개 보유 / ${maybe(today.pending_count, "0")}개 대기`)}${metric("오늘 승률", fmtPct(today.win_rate_pct))}</div></div><div class="kv-grid">${kv("운용 모드", translate(rollout.mode))}${kv("포인터 동기화", runtime.model_pointer_divergence ? "어긋남" : "정상")}${kv("WS 신선도", runtime.ws_public_stale ? "오래됨" : "정상")}${kv("마지막 resume", fmtDateTime((selected.last_resume || {}).generated_at || (selected.last_resume || {}).checked_at || (selected.last_resume || {}).completed_at))}${kv("상태 DB", shortPath(selected.db_path))}${kv("브레이커 사유", activeBreakers.join(" / ") || "없음")}</div><div class="live-sections compact"><section class="section-block compact"><h5>보유 종목</h5><div class="stack">${positionSection}</div></section><section class="section-block compact"><h5>미체결 주문</h5><div class="stack">${orderSection}</div></section><section class="section-block compact"><h5>매도 전략</h5><div class="stack">${planSection}</div></section><section class="section-block compact"><h5>최근 의도</h5><div class="stack">${intentSection}</div></section><section class="section-block compact"><h5>최근 거래 저널</h5><div class="stack">${tradeSection}</div></section></div></article>`;
  }

  function renderWs(snapshot) {
    const ws = snapshot.ws_public || {};
    const health = ws.health_snapshot || {};
    const latestRun = ws.runs_summary_latest || {};
    const lastRxTs = Math.max(
      toNumber(health.updated_at_ms) || 0,
      toNumber((health.last_rx_ts_ms || {}).trade) || 0,
      toNumber((health.last_rx_ts_ms || {}).orderbook) || 0
    );
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
    setTab(state.activeTab, false);
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
  renderAll(INITIAL_SNAPSHOT);
  setTab(state.activeTab, false);
  refresh();
  setInterval(refresh, 10000);
})();

