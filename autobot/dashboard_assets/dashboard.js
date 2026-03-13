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
    EXPECTED_EDGE_NOT_POSITIVE_AFTER_COST: "비용을 빼면 기대수익이 부족함",
    MAX_REPLACES_REACHED: "최대 재호가 횟수에 도달해 주문을 정리",
    ENTRY_ORDER_TIMEOUT: "진입 주문이 시간 초과로 취소",
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
    UPDATED_FROM_CLOSED_ORDERS: "체결 이력 보정 반영",
    CLOSED_ORDERS_BACKFILL: "체결 이력 역보정",
    EXCHANGE_SNAPSHOT: "거래소 스냅샷 반영",
    POSITION_CLOSED: "포지션 종료",
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
    SKIPPED: "건너뜀",
    REJECTED_ADMISSIBILITY: "주문 조건 거절",
    CANCELLED_ENTRY: "진입 취소",
    done_ask_order: "매도 체결",
    managed_exit_order: "관리형 청산",
    missing_on_exchange_after_exit_plan: "청산 후 거래소 미보유 확인",
    external_manual_order: "수동 외부 청산",
    entry_order_timeout: "진입 주문 시간초과",
    verified_exit_order: "체결 확인 완료",
    unverified_position_sync: "포지션 동기화 기반",
    unverified_missing_exit_order: "체결 주문 미확인"
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
    activeLiveLabel: null,
    stream: null,
    fallbackRefreshTimer: null
  };

  const TAB_HERO = {
    overview: {
      eyebrow: "Autobot Terminal",
      title: "Autobot Terminal",
      text: "서비스, 검증, 타이머, 경고를 실시간으로 읽는 운영 화면입니다."
    },
    training: {
      eyebrow: "Training Surface",
      title: "Training & Acceptance",
      text: "후보 생성부터 검증, 챌린저 흐름까지 한 눈에 보는 탭입니다."
    },
    paper: {
      eyebrow: "Paper Runs",
      title: "Paper Experiments",
      text: "최근 페이퍼 런의 제출, 체결, 손익 흐름을 빠르게 비교합니다."
    },
    live: {
      eyebrow: "Live Desk",
      title: "Live & Canary Desk",
      text: "현재 보유, 매도 플랜, 주문 상태, 최근 종료 거래를 실시간으로 확인합니다."
    },
    ws: {
      eyebrow: "Data Plane",
      title: "WS Public Plane",
      text: "수집 연결과 적재 신선도를 읽는 데이터 플레인 화면입니다."
    }
  };

  const WEEKDAY_TEXT = ["일", "월", "화", "수", "목", "금", "토"];

  function isPlainObject(value) {
    return Object.prototype.toString.call(value) === "[object Object]";
  }

  function keyLabel(key) {
    const raw = String(key || "").trim();
    const table = {
      error: "오류",
      message: "메시지",
      detail: "상세",
      details: "상세",
      reason: "사유",
      reason_code: "사유",
      code: "코드",
      status: "상태",
      name: "이름",
      description: "설명",
      fatal_reason: "치명 사유"
    };
    return table[raw] || raw.replace(/_/g, " ");
  }

  function tryParseStructuredText(value) {
    if (typeof value !== "string") return value;
    const text = value.trim();
    if (!text) return value;
    if (!((text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]")))) {
      return value;
    }
    try {
      return JSON.parse(text);
    } catch {
      return value;
    }
  }

  function humanizeStructuredValue(value, depth = 0) {
    const parsed = tryParseStructuredText(value);
    if (parsed == null || parsed === "") return "-";
    if (typeof parsed === "string") {
      const text = parsed.trim();
      if (!text) return "-";
      return REASON_TEXT[text] || POLICY_TEXT[text] || text;
    }
    if (typeof parsed === "number" || typeof parsed === "boolean") {
      return String(parsed);
    }
    if (Array.isArray(parsed)) {
      const items = parsed
        .map((item) => humanizeStructuredValue(item, depth + 1))
        .filter((item) => item && item !== "-");
      if (!items.length) return "-";
      const head = items.slice(0, 4).join(" / ");
      return items.length > 4 ? `${head} 외 ${items.length - 4}건` : head;
    }
    if (isPlainObject(parsed)) {
      const preferredKeys = ["message", "error", "detail", "details", "reason", "reason_code", "code", "status"];
      const preferredValues = preferredKeys
        .map((key) => {
          if (!(key in parsed)) return null;
          const text = humanizeStructuredValue(parsed[key], depth + 1);
          if (!text || text === "-") return null;
          return key === "code" || key === "status" || key === "reason_code"
            ? `${keyLabel(key)} ${text}`
            : text;
        })
        .filter(Boolean);
      if (preferredValues.length) {
        return unique(preferredValues).join(" · ");
      }
      const entries = Object.entries(parsed)
        .filter(([key]) => key !== "ok")
        .slice(0, depth === 0 ? 4 : 3)
        .map(([key, item]) => {
          const text = humanizeStructuredValue(item, depth + 1);
          return !text || text === "-" ? null : `${keyLabel(key)} ${text}`;
        })
        .filter(Boolean);
      return entries.length ? entries.join(" · ") : "-";
    }
    return String(parsed);
  }

  function truncateText(value, limit = 180) {
    const text = String(value == null ? "" : value).trim();
    if (!text) return "-";
    return text.length > limit ? `${text.slice(0, limit - 1)}…` : text;
  }

  function normalizeDisplayValue(value, limit = 180) {
    return truncateText(humanizeStructuredValue(value), limit);
  }

  function esc(value) {
    return String(normalizeDisplayValue(value))
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function maybe(value, fallback = "-") {
    return value == null || value === "" ? fallback : value;
  }

  function translate(value) {
    return humanizeStructuredValue(value);
  }

  function unique(values) {
    return [...new Set((values || []).filter(Boolean))];
  }

  function toNumber(value) {
    if (value == null) return null;
    if (typeof value === "string" && value.trim() === "") return null;
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

  function pad2(value) {
    return String(Math.trunc(Number(value) || 0)).padStart(2, "0");
  }

  function toDateObject(value) {
    if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
    const ts = coerceTs(value);
    if (ts != null) return new Date(ts);
    const text = String(value || "").trim();
    if (!text) return null;
    const dateOnly = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (dateOnly) {
      return new Date(Number(dateOnly[1]), Number(dateOnly[2]) - 1, Number(dateOnly[3]), 12, 0, 0, 0);
    }
    const parsed = Date.parse(text);
    if (Number.isNaN(parsed)) return null;
    return new Date(parsed);
  }

  function relativeTimeLabel(ts) {
    const deltaMs = ts - Date.now();
    const absSec = Math.abs(deltaMs) / 1000;
    if (absSec < 30) return "방금";
    if (absSec < 3600) return `${Math.round(absSec / 60)}분 ${deltaMs >= 0 ? "뒤" : "전"}`;
    if (absSec < 86400) return `${Math.round(absSec / 3600)}시간 ${deltaMs >= 0 ? "뒤" : "전"}`;
    return `${Math.round(absSec / 86400)}일 ${deltaMs >= 0 ? "뒤" : "전"}`;
  }

  function formatAbsoluteDateTime(date, options = {}) {
    const now = new Date();
    const includeYear = options.includeYear == null ? date.getFullYear() !== now.getFullYear() : Boolean(options.includeYear);
    const parts = [];
    if (includeYear) parts.push(`${date.getFullYear()}년`);
    parts.push(`${date.getMonth() + 1}월`);
    parts.push(`${date.getDate()}일`);
    parts.push(`(${WEEKDAY_TEXT[date.getDay()]})`);
    if (options.includeTime !== false) {
      parts.push(`${pad2(date.getHours())}:${pad2(date.getMinutes())}`);
    }
    return parts.join(" ");
  }

  function fmtDateTime(value, options = {}) {
    const date = toDateObject(value);
    if (!date) return normalizeDisplayValue(value, 220);
    const base = formatAbsoluteDateTime(date, options);
    return options.includeRelative === false ? base : `${base} · ${relativeTimeLabel(date.getTime())}`;
  }

  function fmtDateLabel(value) {
    const date = toDateObject(value);
    if (!date) return normalizeDisplayValue(value, 120);
    return formatAbsoluteDateTime(date, { includeTime: false });
  }

  function fmtCompactDateTime(value) {
    const date = toDateObject(value);
    if (!date) return "-";
    const now = new Date();
    const includeYear = date.getFullYear() !== now.getFullYear();
    const datePart = includeYear
      ? `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`
      : `${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
    return `${datePart} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`;
  }

  function fmtAge(value) {
    const date = toDateObject(value);
    if (!date) return "-";
    return relativeTimeLabel(date.getTime());
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

  function terminalTable(headers, rows, rowClass = "") {
    const head = `<div class="terminal-head">${headers.map((item) => `<div class="terminal-cell">${esc(item)}</div>`).join("")}</div>`;
    const body = rows.length
      ? rows.map((row) => `<div class="terminal-row ${rowClass} ${row.rowClass || ""}">${row.cells.join("")}</div>`).join("")
      : "";
    return `<div class="terminal-table">${head}${body}</div>`;
  }

  function cell(primary, secondary = "", extraClass = "", align = "") {
    return `<div class="terminal-cell ${extraClass} ${align}"><strong>${esc(primary)}</strong>${secondary ? `<span>${esc(secondary)}</span>` : ""}</div>`;
  }

  function noteCard(title, text, kind = "neutral") {
    return `<article class="alert-card"><div class="row"><h4>${esc(title)}</h4>${pill("메모", kind === "bad" ? "주의" : kind === "warn" ? "참고" : "요약", kind)}</div><p>${esc(text)}</p></article>`;
  }

  function compactStat(label, value, tone = "") {
    return `<div class="list-meta-item ${tone}"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;
  }

  function compactRow({ title, summary = "", items = [], pillHtml = "", extraClass = "" }) {
    return `<article class="list-row ${extraClass}"><div class="list-row-head"><div><h4>${esc(title)}</h4>${summary ? `<p class="list-row-summary">${esc(summary)}</p>` : ""}</div>${pillHtml}</div>${items.length ? `<div class="list-meta">${items.join("")}</div>` : ""}</article>`;
  }

  function toneFromValue(value) {
    const num = toNumber(value);
    if (num == null || num === 0) return "neutral";
    return num > 0 ? "good" : "bad";
  }

  function signalCard({ label, value, note = "", tone = "neutral" }) {
    return `<article class="live-signal-card ${tone}"><span class="live-signal-label">${esc(label)}</span><strong>${esc(value)}</strong>${note ? `<p>${esc(note)}</p>` : ""}</article>`;
  }

  function statusChip(label, value, tone = "neutral") {
    return `<span class="live-status-chip ${tone}"><b>${esc(label)}</b><strong>${esc(value)}</strong></span>`;
  }

  function surfaceCard({ title, copy = "", body = "", extraClass = "" }) {
    return `<article class="live-surface-card ${extraClass}"><div class="live-surface-head"><div><h5>${esc(title)}</h5>${copy ? `<p class="live-surface-copy">${esc(copy)}</p>` : ""}</div></div>${body}</article>`;
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
    const expectedEs = fmtBps(intent.trade_action_expected_es_bps);
    const multiple = fmtNumber(intent.trade_action_notional_multiplier, 2);
    if (!action || action === "-") return "학습된 trade action 정보가 아직 없습니다.";
    return `${translate(action)} 전략으로 판단했고, 기대 순엣지는 ${edge}, 예상 하방은 ${downside}, 예상 ES는 ${expectedEs}, 진입 금액 배수는 ${multiple}배로 계산됐습니다.`;
  }

  function intentNarrative(intent) {
    const market = intent.market || "이 종목";
    const side = translate(intent.side);
    const status = translate(intent.status);
    if (intent.status === "REJECTED_ADMISSIBILITY") {
      const reason = translate(intent.skip_reason) || "주문 조건";
      return `${market} ${side} 주문은 ${reason} 때문에 거절됐습니다.`;
    }
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

  function formatErrorMessage(value) {
    const text = normalizeDisplayValue(value, 240);
    return text === "-" ? "알 수 없는 오류" : text;
  }

  function compactPlanSummary(plan) {
    if (!plan) return "청산 플랜 없음";
    const bits = [translate(plan.exit_mode)];
    if (plan.exit_mode === "hold") {
      if (toNumber(plan.hold_remaining_minutes) != null) bits.push(`${Math.max(0, Number(plan.hold_remaining_minutes))}분 남음`);
      if (plan.timeout_ts_ms) bits.push(fmtCompactDateTime(plan.timeout_ts_ms));
      return bits.join(" · ");
    }
    if (plan.tp_enabled) bits.push(`익절 ${fmtPct(Number(plan.tp_pct))}`);
    if (plan.sl_enabled) bits.push(`손절 ${fmtPct(Number(plan.sl_pct))}`);
    if (plan.trailing_enabled) bits.push(`추적 ${fmtPct(Number(plan.trail_pct))}`);
    if (plan.timeout_ts_ms) bits.push(`종료 ${fmtCompactDateTime(plan.timeout_ts_ms)}`);
    return bits.join(" · ");
  }

  function compactIntentSummary(intent) {
    const bits = [fmtCompactDateTime(intent.ts_ms)];
    if (intent.skip_reason) bits.push(translate(intent.skip_reason));
    else bits.push(translate(intent.status));
    if (toNumber(intent.expected_net_edge_bps) != null) bits.push(`순엣지 ${fmtBps(intent.expected_net_edge_bps)}`);
    if (intent.trade_action_recommended_action) bits.push(`액션 ${translate(intent.trade_action_recommended_action)}`);
    return bits.join(" · ");
  }

  function shortLiveLabel(label) {
    const text = String(label || "").trim();
    if (!text) return "-";
    if (text.includes("후보")) return "카나리아";
    if (text.includes("레거시")) return "레거시";
    if (text.includes("메인")) return "메인";
    return text.replace(/\s+/g, "");
  }

  function setError(message) {
    const node = document.getElementById("fetch-error");
    if (!message) {
      node.hidden = true;
      node.innerHTML = "";
      return;
    }
    const detail = formatErrorMessage(message);
    node.hidden = false;
    node.innerHTML = `<strong>실시간 연결 이슈</strong><span>${esc(detail)}</span>`;
  }

  function renderHero() {
    const copy = TAB_HERO[state.activeTab] || TAB_HERO.overview;
    const eyebrow = document.getElementById("hero-eyebrow");
    const title = document.getElementById("hero-title");
    const text = document.getElementById("hero-text");
    if (eyebrow) eyebrow.textContent = copy.eyebrow;
    if (title) title.textContent = copy.title;
    if (text) text.textContent = copy.text;
  }

  function setTab(nextTab, updateHash = true, options = {}) {
    if (!TABS.has(nextTab)) return;
    const scroll = options.scroll !== false;
    state.activeTab = nextTab;
    document.querySelectorAll(".tab-button").forEach((button) => {
      const active = button.dataset.tab === nextTab;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.querySelectorAll(".quick-tab-button").forEach((button) => {
      const active = button.dataset.tab === nextTab;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.querySelectorAll(".pane").forEach((pane) => {
      pane.hidden = pane.dataset.pane !== nextTab;
    });
    renderHero();
    if (updateHash) history.replaceState(null, "", `#${nextTab}`);
    if (scroll) {
      const workspace = document.querySelector(".workspace");
      if (workspace && typeof workspace.scrollTo === "function") workspace.scrollTo({ top: 0, left: 0, behavior: "auto" });
      window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    }
  }

  function bindTabs() {
    document.getElementById("tab-bar").addEventListener("click", (event) => {
      const button = event.target.closest(".tab-button");
      if (!button) return;
      event.preventDefault();
      setTab(button.dataset.tab);
      setDrawerOpen(false);
    });
    const quickTabs = document.getElementById("mobile-quick-tabs");
    if (quickTabs) {
      quickTabs.addEventListener("click", (event) => {
        const button = event.target.closest(".quick-tab-button");
        if (!button) return;
        event.preventDefault();
        setTab(button.dataset.tab);
      });
    }
    window.addEventListener("hashchange", () => {
      const nextTab = location.hash.replace("#", "");
      if (TABS.has(nextTab)) setTab(nextTab, false);
    });
  }

  function setDrawerOpen(open) {
    const shell = document.getElementById("app-shell");
    const scrim = document.getElementById("nav-scrim");
    const toggle = document.getElementById("menu-toggle");
    if (!shell || !scrim || !toggle) return;
    shell.classList.toggle("nav-open", Boolean(open));
    scrim.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
  }

  function bindLayout() {
    const toggle = document.getElementById("menu-toggle");
    const close = document.getElementById("menu-close");
    const scrim = document.getElementById("nav-scrim");
    if (toggle) {
      toggle.addEventListener("click", () => {
        const shell = document.getElementById("app-shell");
        const next = !(shell && shell.classList.contains("nav-open"));
        setDrawerOpen(next);
      });
    }
    if (close) {
      close.addEventListener("click", () => setDrawerOpen(false));
    }
    if (scrim) {
      scrim.addEventListener("click", () => setDrawerOpen(false));
    }
    window.addEventListener("resize", () => {
      if (window.innerWidth > 920) setDrawerOpen(false);
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
    document.getElementById("services-grid").innerHTML = terminalTable(
      ["서비스", "상태", "최근 시작", "다음 실행"],
      Object.entries(services).map(([key, svc]) => {
        const active = String(svc.active_state || "").toLowerCase();
        const sub = String(svc.sub_state || "").toLowerCase();
        return {
          cells: [
            cell(SERVICE_LABELS[key] || key, svc.description || ""),
            cell(`${translate(active)} / ${translate(sub)}`),
            cell(fmtCompactDateTime(svc.started_at)),
            cell(fmtCompactDateTime(svc.next_run_at)),
          ],
        };
      }),
    ) || empty("표시할 서비스가 없습니다.");

    const notes = [];
    if ((acceptance.reasons || []).length) notes.push(compactRow({
      title: "직접 사유",
      summary: unique(acceptance.reasons).map(translate).join(" / "),
      pillHtml: pill("메모", "참고", "warn"),
    }));
    if ((acceptance.trainer_reasons || []).length) notes.push(compactRow({
      title: "학습 증거",
      summary: unique(acceptance.trainer_reasons).map(translate).join(" / "),
      pillHtml: pill("메모", "참고", "warn"),
    }));
    if (challenger.reason) notes.push(compactRow({
      title: "챌린저 미기동 사유",
      summary: translate(challenger.reason),
      pillHtml: pill("메모", "참고", "warn"),
    }));
    liveStates.forEach((liveState) => {
      const breakers = unique((liveState.active_breakers || []).map((item) => item.reason || item.code || item.name));
      if (breakers.length) notes.push(compactRow({
        title: `${liveState.label} 브레이커`,
        summary: breakers.map(translate).join(" / "),
        pillHtml: pill("메모", "주의", "bad"),
      }));
    });
    document.getElementById("alerts-grid").innerHTML =
      notes.length ? `<div class="dense-list">${notes.join("")}</div>` : compactRow({
        title: "현재 상태",
        summary: "즉시 확인이 필요한 경고는 없습니다.",
      });
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
    document.getElementById("training-details").innerHTML = `<div class="dense-list">${
      [
        compactRow({
          title: "이번 후보 해석",
          summary: acceptanceNarrative,
          items: [
            compactStat("모델 계열", maybe(acceptance.model_family)),
            compactStat("후보 run", shortRun(acceptance.candidate_run_id)),
            compactStat("이전 챔피언", shortRun(acceptance.champion_before_run_id)),
            compactStat("현재 챔피언", shortRun(acceptance.champion_after_run_id)),
            compactStat("백테스트", boolLabel(acceptance.backtest_pass)),
            compactStat("페이퍼", boolLabel(acceptance.paper_pass)),
          ],
        }),
        compactRow({
          title: "챌린저 루프",
          summary: challengerNarrative,
          items: [
            compactStat("챌린저 시작", challenger.started ? "시작됨" : "미시작"),
            compactStat("멈춘 이유", translate(challenger.reason)),
            compactStat("추가 메모", joinTranslated(challenger.acceptance_notes || [])),
            compactStat("보고서", shortPath(challenger.artifact_path)),
          ],
        }),
        compactRow({
          title: "랭크 그림자 레인",
          summary: rankNarrative,
          items: [
            compactStat("현재 상태", maybe(rankShadow.status)),
            compactStat("다음 액션", maybe(rankShadow.next_action)),
            compactStat("선택 레인", maybe((rankShadow.governance_action || {}).selected_lane_id)),
            compactStat("선택 스크립트", maybe((rankShadow.governance_action || {}).selected_acceptance_script)),
            compactStat("후보 run", shortRun(rankShadow.candidate_run_id)),
            compactStat("사이클 보고서", shortPath(rankShadow.artifact_path)),
          ],
        }),
      ].join("")
    }</div>`;

    const artifacts = training.candidate_artifacts || {};
    const runtime = artifacts.runtime_recommendations || {};
    const policy = artifacts.selection_policy || {};
    const calibration = artifacts.selection_calibration || {};
    const budget = artifacts.search_budget_decision || {};
    const wf = artifacts.walk_forward_report || {};
    const tradeAction = runtime.trade_action || {};
    const tradeActionSample = (tradeAction.sample_bins || [])[0] || {};

    document.getElementById("artifact-grid").innerHTML = `<div class="dense-list">${
      [
        compactRow({
          title: "실전 주문 추천",
          summary: runtimeExplain(runtime),
          items: [
            compactStat("기본 청산", translate(runtime.recommended_exit_mode)),
            compactStat("선택 family", maybe(runtime.chosen_family)),
            compactStat("선택 rule", maybe(runtime.chosen_rule_id)),
            compactStat("hold family", maybe((runtime.hold_family || {}).status)),
            compactStat("risk family", maybe((runtime.risk_family || {}).status)),
            compactStat("family compare", maybe((runtime.family_compare || {}).status)),
          ],
        }),
        compactRow({
          title: "진입 선택 규칙",
          summary: `후보를 고를 때는 ${translate(policy.mode)} 방식을 쓰고, 점수 보정은 ${maybe(calibration.method)} 기준으로 적용합니다.`,
          items: [
            compactStat("선택 방식", translate(policy.mode)),
            compactStat("기준 키", maybe(policy.threshold_key)),
            compactStat("순위 비율", policy.rank_quantile == null ? "-" : fmtPct(Number(policy.rank_quantile) * 100)),
            compactStat("보정 방식", maybe(calibration.method)),
          ],
        }),
        compactRow({
          title: "Trade Action 정책",
          summary: tradeAction.status === "ready"
            ? `현재 예시 bin에서는 ${translate(tradeActionSample.recommended_action)} 전략과 ${fmtBps(tradeActionSample.expected_edge_bps)} 기대 엣지를 사용합니다.`
            : "trade action 정책이 아직 준비되지 않았습니다.",
          items: [
            compactStat("정책 상태", maybe(tradeAction.status)),
            compactStat("리스크 변수", maybe(tradeAction.risk_feature_name)),
            compactStat("hold 추천 bin", maybe(tradeAction.hold_bins_recommended)),
            compactStat("risk 추천 bin", maybe(tradeAction.risk_bins_recommended)),
            compactStat("예시 엣지", fmtBps(tradeActionSample.expected_edge_bps)),
            compactStat("예시 진입 배수", fmtNumber(tradeActionSample.notional_multiplier, 2)),
          ],
        }),
        compactRow({
          title: "검증과 예산",
          summary: `이번 후보는 ${maybe(wf.windows_run)}개 검증 구간과 ${maybe(wf.selection_search_trial_count)}개 선택 실험을 바탕으로 평가됐고, 검색 예산은 ${maybe(budget.decision_mode)} 모드로 적용됐습니다.`,
          items: [
            compactStat("White 검정", boolLabel(wf.white_rc_comparable)),
            compactStat("Hansen 검정", boolLabel(wf.hansen_spa_comparable)),
            compactStat("선택 실험 수", maybe(wf.selection_search_trial_count)),
            compactStat("예산 모드", maybe(budget.decision_mode)),
            compactStat("부스터 시도 수", maybe(budget.booster_sweep_trials)),
            compactStat("예산 메모", joinTranslated(budget.reasons || [])),
          ],
        }),
      ].join("")
    }</div>`;
  }

  function renderPaper(snapshot) {
    const rows = [...((snapshot.paper || {}).recent_runs || [])].sort((a, b) => {
      return (coerceTs(b.updated_at) || 0) - (coerceTs(a.updated_at) || 0);
    });
    document.getElementById("paper-grid").innerHTML = rows.length
      ? terminalTable(
        ["런", "제출", "체결", "체결률", "실현 손익", "업데이트"],
        rows.map((run) => ({
          cells: [
            cell(shortRun(run.run_id), `${maybe(run.feature_provider)} / ${maybe(run.micro_provider)}`),
            cell(maybe(run.orders_submitted, "0")),
            cell(maybe(run.orders_filled, "0")),
            cell(run.fill_rate == null ? "-" : fmtPct(Number(run.fill_rate) * 100)),
            cell(fmtMoney(run.realized_pnl_quote), `평가 ${fmtMoney(run.unrealized_pnl_quote)}`, Number(run.realized_pnl_quote || 0) > 0 ? "good" : Number(run.realized_pnl_quote || 0) < 0 ? "bad" : "", "right"),
            cell(fmtDateTime(run.updated_at)),
          ],
        })),
      )
      : empty("최근 페이퍼 런이 없습니다.");
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

  function liveStateServiceKey(item) {
    const label = String((item || {}).label || "");
    if (label.includes("후보")) return "live_candidate";
    if (label.includes("메인")) return "live_main";
    return null;
  }

  function liveStateService(snapshot, item) {
    const key = liveStateServiceKey(item);
    return key ? ((snapshot.services || {})[key] || {}) : {};
  }

  function liveStateHasActivity(item) {
    return Boolean(
      Number(item.positions_count || 0) > 0 ||
      Number(item.open_orders_count || 0) > 0 ||
      Number(item.active_risk_plans_count || 0) > 0 ||
      Number(item.intents_count || 0) > 0 ||
      item.breaker_active
    );
  }

  function isFreshLiveState(item, maxAgeMs = 15 * 60 * 1000) {
    const ts = coerceTs(item.updated_at);
    return ts != null && (Date.now() - ts) <= maxAgeMs;
  }

  function shouldDisplayLiveState(snapshot, item) {
    const service = liveStateService(snapshot, item);
    const active = String(service.active_state || "").trim().toLowerCase() === "active";
    if (active) return true;
    if (liveStateHasActivity(item)) return true;
    return isFreshLiveState(item);
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
    if (plan.tp_enabled) parts.push(`익절 ${fmtPct(Number(plan.tp_pct))}`);
    if (plan.sl_enabled) parts.push(`손절 ${fmtPct(Number(plan.sl_pct))}`);
    if (plan.trailing_enabled) parts.push(`추적 ${fmtPct(Number(plan.trail_pct))}`);
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
    const allStates = [...((snapshot.live || {}).states || [])].sort((a, b) => statePriority(b) - statePriority(a));
    const visibleStates = allStates.filter((item) => shouldDisplayLiveState(snapshot, item));
    const states = visibleStates.length ? visibleStates : allStates;
    const summaryStrip = document.getElementById("live-summary-strip");
    const tabBar = document.getElementById("live-tab-bar");
    const container = document.getElementById("live-state-list");

    if (!state.activeLiveLabel || !states.some((item) => item.label === state.activeLiveLabel)) {
      state.activeLiveLabel = (states[0] || {}).label || null;
    }

    summaryStrip.innerHTML = states.length
      ? states.map((liveState) => {
        const active = liveState.label === state.activeLiveLabel;
        const todayState = liveState.today_trade_summary || {};
        const privateWsTs = coerceTs((liveState.last_ws_event || {}).event_ts_ms) || coerceTs((liveState.daemon_last_run || {}).private_ws_last_event_ts_ms);
        const unit = liveStateService(snapshot, liveState);
        const unitActive = String(unit.active_state || "").trim().toLowerCase() === "active";
        const selectorTone = liveState.breaker_active
          ? "bad"
          : Number(liveState.positions_count || 0) > 0
            ? "good"
            : Number(liveState.open_orders_count || 0) > 0
              ? "warn"
              : unitActive
                ? "neutral"
                : "neutral";
        const selectorHeadline = liveState.breaker_active
          ? "브레이커 감지"
          : Number(liveState.positions_count || 0) > 0
            ? "포지션 운용 중"
            : Number(liveState.open_orders_count || 0) > 0
              ? "주문 감시 중"
              : unitActive
                ? "관찰 모드"
                : "비활성 기록";
        const selectorMeta = `${unitActive ? "가동" : "중지"} · WS ${privateWsTs == null ? "없음" : privateWsFreshness}`;
        return `
          <button class="live-selector-card ${active ? "active" : ""}" type="button" data-live-label="${esc(liveState.label)}">
            <div class="live-selector-main">
              <div class="live-selector-copy">
                <div class="live-selector-top">
                  <div>
                    <span class="live-selector-label">${esc(shortLiveLabel(liveState.label))}</span>
                    <strong>${esc(selectorHeadline)}</strong>
                  </div>
                  ${pill("모드", translate((liveState.rollout_status || {}).mode), selectorTone)}
                </div>
                <div class="live-selector-foot">${esc(selectorMeta)}</div>
              </div>
              <div class="live-selector-kpis">
                <div><span>보유</span><strong>${esc(`${maybe(liveState.positions_count, "0")}개`)}</strong></div>
                <div><span>주문</span><strong>${esc(`${maybe(liveState.open_orders_count, "0")}개`)}</strong></div>
                <div><span>손익</span><strong>${esc(fmtMoney(todayState.net_pnl_quote_total))}</strong></div>
              </div>
            </div>
          </button>
        `;
      }).join("")
      : empty("라이브 상태가 없습니다.");

    summaryStrip.onclick = (event) => {
      const button = event.target.closest(".live-selector-card");
      if (!button) return;
      event.preventDefault();
      setLiveLabel(button.dataset.liveLabel);
      renderLive(snapshot);
    };

    tabBar.innerHTML = "";
    tabBar.hidden = true;

    const selected = states.find((item) => item.label === state.activeLiveLabel) || states[0];
    if (!selected) {
      container.innerHTML = empty("라이브 상태 DB를 찾지 못했습니다.");
      return;
    }
    if (selected.exists === false) {
      container.innerHTML = empty(`${selected.label || "라이브 상태"} DB를 찾지 못했습니다. ${selected.db_path || ""}`.trim());
      return;
    }

    const selectedService = liveStateService(snapshot, selected);
    const selectedServiceActive = String(selectedService.active_state || "").trim().toLowerCase() === "active";
    const runtime = selected.runtime_health || {};
    const daemonLastRun = selected.daemon_last_run || {};
    const lastWsEvent = selected.last_ws_event || {};
    const rollout = selected.rollout_status || {};
    const positions = [...(selected.positions || [])];
    const openOrders = [...(selected.open_orders || [])].sort((a, b) => (coerceTs(b.updated_ts) || 0) - (coerceTs(a.updated_ts) || 0));
    const riskPlans = [...(selected.active_risk_plans || [])].sort((a, b) => (coerceTs(b.updated_ts) || 0) - (coerceTs(a.updated_ts) || 0));
    const intents = [...(selected.recent_intents || [])].sort((a, b) => (coerceTs(b.ts_ms) || 0) - (coerceTs(a.ts_ms) || 0));
    const today = selected.today_trade_summary || {};
    const recentTrades = [...(selected.recent_trades || [])].sort((a, b) => {
      const aTs = coerceTs(a.exit_ts_ms) || coerceTs(a.entry_ts_ms) || coerceTs(a.updated_ts) || 0;
      const bTs = coerceTs(b.exit_ts_ms) || coerceTs(b.entry_ts_ms) || coerceTs(b.updated_ts) || 0;
      return bTs - aTs;
    });
    const activeBreakers = unique((selected.active_breakers || []).flatMap((item) => {
      const codes = Array.isArray(item.reason_codes) ? item.reason_codes : [];
      return codes.length ? codes : [item.reason || item.code || item.name || item.breaker_key];
    })).map(translate);
    const primaryPosition = positions[0];
    const primaryPlan = riskPlans.find((item) => item.market === (primaryPosition || {}).market) || riskPlans[0];
    const primaryOrder = openOrders.find((item) => item.market === (primaryPosition || {}).market && item.side === "ask") || openOrders[0];
    const isCanary = String(rollout.mode || "").trim().toLowerCase() === "canary" || String(selected.label || "").includes("카나리아");
    const pointerStatus = runtime.model_pointer_divergence
      ? (isCanary ? "후보 모델 추적 중" : "모델 포인터 불일치")
      : (isCanary ? "챔피언과 동일" : "정상");
    const privateWsLastTs = coerceTs(lastWsEvent.event_ts_ms) || coerceTs(daemonLastRun.private_ws_last_event_ts_ms);
    const privateWsEventsTotal = maybe(daemonLastRun.private_ws_events_total, "0");
    const privateWsFreshness = privateWsLastTs == null ? "아직 수신 없음" : fmtAge(privateWsLastTs);
    const verifiedClosed = maybe(today.verified_closed_count, "0");
    const unverifiedClosed = maybe(today.unverified_closed_count, "0");
    const topSummary = primaryPosition && primaryPlan
      ? `${primaryPosition.market || "-"} · ${compactPlanSummary(primaryPlan)}`
      : positions.length
        ? `${positions.length}개 포지션 운용 중`
        : "포지션 없이 관찰 중";
    const liveNarrative = positions.length
      ? `${positions.length}개 포지션 · ${maybe(selected.open_orders_count, "0")}개 주문`
      : `${maybe(selected.open_orders_count, "0")}개 주문 감시 중`;
    const todayLabel = fmtDateLabel(today.date_label);
    const todaySummaryLine = `${todayLabel} ${maybe(today.timezone, "KST")} 세션`;
    const todaySummaryTags = [
      `${maybe(today.closed_count, "0")}건 종료`,
      `승 ${maybe(today.wins, "0")} / 패 ${maybe(today.losses, "0")} / 보합 ${maybe(today.flats, "0")}`,
      `순손익 ${fmtMoney(today.net_pnl_quote_total)}`,
    ];
    const leadTone = selected.breaker_active
      ? "bad"
      : positions.length
        ? "good"
        : openOrders.length
          ? "warn"
          : selectedServiceActive
            ? "neutral"
            : "neutral";
    const spotlightValue = primaryPosition ? fmtPct(primaryPosition.unrealized_pnl_pct) : fmtMoney(today.net_pnl_quote_total);
    const spotlightTone = primaryPosition ? toneFromValue(primaryPosition.unrealized_pnl_pct) : toneFromValue(today.net_pnl_quote_total);
    const spotlightTags = primaryPosition
      ? [
        `보유 ${fmtNumber(primaryPosition.base_amount, 8)}개`,
        `평균 ${fmtMoney(primaryPosition.avg_entry_price)}`,
        `현재가 ${fmtMoney(primaryPosition.current_price)}`,
        `평가손익 ${fmtMoney(primaryPosition.unrealized_pnl_quote)}`,
        primaryPlan ? compactPlanSummary(primaryPlan) : "청산 플랜 없음",
      ]
      : [
        `오늘 종료 ${maybe(today.closed_count, "0")}건`,
        `진입 대기 ${maybe(today.current_pending_orders_count, "0")}건`,
        `최근 갱신 ${fmtCompactDateTime(selected.updated_at)}`,
      ];

    const runtimeSignals = [
      statusChip("서비스", selectedServiceActive ? "가동" : "중지", selectedServiceActive ? "good" : "neutral"),
      statusChip("모드", translate(rollout.mode), leadTone),
      statusChip("주문", boolLabel(rollout.order_emission_allowed), rollout.order_emission_allowed ? "good" : "bad"),
      statusChip("공용 WS", runtime.ws_public_stale ? "지연" : "정상", runtime.ws_public_stale ? "warn" : "good"),
      statusChip("개인 WS", privateWsFreshness, privateWsLastTs == null ? "warn" : "good"),
      statusChip("브레이커", activeBreakers.length ? `${activeBreakers.length}건` : "없음", activeBreakers.length ? "bad" : "neutral"),
    ].join("");

    const issueRail = [
      selected.error ? `<article class="live-inline-banner bad"><strong>상태 DB 오류</strong><span>${esc(selected.error)}</span></article>` : "",
      !selectedServiceActive ? `<article class="live-inline-banner neutral"><strong>비활성 상태 기록</strong><span>${esc(`${selected.label} 서비스는 현재 중지돼 있습니다. 혼동을 줄이기 위해 최근 활동이 없는 비활성 레인은 기본적으로 숨기고 있습니다.`)}</span></article>` : "",
      activeBreakers.length ? `<article class="live-inline-banner warn"><strong>즉시 확인할 브레이커</strong><span>${esc(activeBreakers.join(" / "))}</span></article>` : "",
    ].filter(Boolean).join("");

    const intentSection = intents.length
      ? `<div class="live-intent-list">${intents.slice(0, 3).map((intent) => {
        const tone = intent.status === "REJECTED_ADMISSIBILITY" || intent.skip_reason
          ? "bad"
          : intent.side === "ask"
            ? "warn"
            : "good";
        return `
          <article class="live-intent-card">
            <div class="live-intent-head">
              <div>
                <strong>${esc(`${intent.market || "-"} · ${translate(intent.side)}`)}</strong>
                <p>${esc(compactIntentSummary(intent))}</p>
              </div>
              ${pill("상태", translate(intent.status), tone)}
            </div>
            <div class="live-intent-foot">${esc(intentNarrative(intent))}</div>
          </article>
        `;
      }).join("")}</div>`
      : empty("최근 의사결정 로그가 없습니다.");

    const unlinkedPlans = riskPlans.filter((plan) => !positions.some((position) => position.market === plan.market));

    const positionTiles = positions.length
      ? `<div class="position-tiles">${positions.map((position) => {
        const linkedPlan = riskPlans.find((item) => item.market === position.market);
        const linkedOrder = openOrders.find((item) => item.market === position.market);
        return `
          <article class="position-tile">
            <div class="position-tile-head">
              <div>
                <strong>${esc(position.market || "-")}</strong>
                <p>${esc(`보유 ${fmtNumber(position.base_amount, 8)}개 · 갱신 ${fmtCompactDateTime(position.updated_ts)}`)}</p>
              </div>
              ${pill("실시간", position.current_price == null ? "지연" : "동기화", position.current_price == null ? "warn" : "good")}
            </div>
            <div class="position-tile-grid">
              ${metric("현재가", fmtMoney(position.current_price))}
              ${metric("평균 매수가", fmtMoney(position.avg_entry_price))}
              ${metric("현재 수익률", fmtPct(position.unrealized_pnl_pct))}
              ${metric("평가손익", fmtMoney(position.unrealized_pnl_quote))}
            </div>
            <p class="position-plan-copy">${esc(linkedPlan ? compactPlanSummary(linkedPlan) : "청산 플랜 없음")}</p>
            <div class="plan-tags">
              <span class="plan-chip ${toneFromValue(position.unrealized_pnl_quote)}">${esc(`손익 ${fmtMoney(position.unrealized_pnl_quote)}`)}</span>
              <span class="plan-chip">${esc(linkedPlan ? translate(linkedPlan.state) : "플랜 없음")}</span>
              <span class="plan-chip">${esc(linkedOrder ? `${translate(linkedOrder.side)} 주문 대기` : "주문 없음")}</span>
            </div>
          </article>
        `;
      }).join("")}</div>`
      : "";

    const unlinkedPlanSection = unlinkedPlans.length
      ? `<div class="plan-board">${unlinkedPlans.map((plan) => `
        <div class="plan-row">
          <div class="plan-row-header">
            <div>
              <strong>${esc(`${plan.market || "-"} · ${translate(plan.exit_mode)}`)}</strong>
              <p>${esc(compactPlanSummary(plan))}</p>
            </div>
            ${pill("플랜", translate(plan.state), String(plan.state).toUpperCase() === "ACTIVE" ? "good" : "warn")}
          </div>
        </div>
      `).join("")}</div>`
      : "";

    const positionSection = positionTiles || unlinkedPlanSection
      ? `${positionTiles}${unlinkedPlanSection}`
      : empty("보유 종목과 청산 플랜이 없습니다.");

    const orderSection = openOrders.length
      ? `<div class="order-stack">${openOrders.map((order) => `
        <article class="order-card">
          <div class="order-card-head">
            <div>
              <strong>${esc(`${order.market || "-"} · ${translate(order.side)}`)}</strong>
              <p>${esc(`${translate(order.ord_type)} · 상태 ${translate(order.raw_exchange_state || order.local_state || order.state)}`)}</p>
            </div>
            ${pill("대기", fmtNumber((toNumber(order.volume_req) || 0) - (toNumber(order.volume_filled) || 0), 8), Number((toNumber(order.volume_req) || 0) - (toNumber(order.volume_filled) || 0)) > 0 ? "warn" : "neutral")}
          </div>
          <div class="order-card-grid">
            ${metric("주문가", fmtMoney(order.price))}
            ${metric("요청 수량", fmtNumber(order.volume_req, 8))}
            ${metric("체결 수량", fmtNumber(order.volume_filled, 8))}
            ${metric("최근 갱신", fmtCompactDateTime(order.updated_ts))}
          </div>
        </article>
      `).join("")}</div>`
      : empty("미체결 주문이 없습니다.");

    const tradeSection = recentTrades.length
      ? terminalTable(
        ["거래", "종료 시각", "보유 시간", "체결 확인", "순손익", "종료 방식"],
        recentTrades.slice(0, 8).map((trade) => {
          const direction = trade.status === "CLOSED" ? "거래 종료" : trade.status;
          const pnlText = trade.realized_pnl_quote == null
            ? (trade.close_verified === false ? "체결 확인 전" : "계산 전")
            : `${fmtMoney(trade.realized_pnl_quote)} / ${fmtPct(trade.realized_pnl_pct)}`;
          const durationText = trade.hold_minutes == null ? "계산 전" : `${trade.hold_minutes}분`;
          return {
            rowClass: Number(trade.realized_pnl_quote || 0) > 0 ? "positive" : Number(trade.realized_pnl_quote || 0) < 0 ? "negative" : "",
            cells: [
              cell(`${trade.market || "-"} · ${direction}`, `${translate(trade.entry_reason_code)} → ${translate(trade.close_reason_code)}`),
              cell(fmtCompactDateTime(trade.exit_ts_ms)),
              cell(durationText),
              cell(trade.close_verified === false ? "미확정" : trade.close_verified === true ? "확정" : "-"),
              cell(pnlText, trade.exit_price == null ? "" : `종료가 ${fmtMoney(trade.exit_price)}`, Number(trade.realized_pnl_quote || 0) > 0 ? "good" : Number(trade.realized_pnl_quote || 0) < 0 ? "bad" : "", "right"),
              cell(translate(trade.close_mode)),
            ],
          };
        }),
      )
      : empty("아직 거래 저널이 없습니다.");

    container.innerHTML = `
      <div class="live-dashboard">
        <section class="live-command-shell">
          <article class="live-hero-card ${leadTone}">
            <div class="live-hero-head">
              <div>
                <p class="eyebrow">Live Command</p>
                <h4>${esc(selected.label)}</h4>
                <p class="section-copy">${esc(liveNarrative)}</p>
              </div>
              <div class="live-pill-stack">
                ${pill("브레이커", boolLabel(selected.breaker_active), selected.breaker_active ? "bad" : "good")}
                ${pill("주문 허용", boolLabel(rollout.order_emission_allowed), rollout.order_emission_allowed ? "good" : "bad")}
              </div>
            </div>
            <div class="live-hero-grid">
              <article class="live-spotlight ${spotlightTone}">
                <span class="live-spotlight-kicker">${esc(primaryPosition ? "핵심 포지션" : "세션 개요")}</span>
                <strong class="live-spotlight-market">${esc(primaryPosition ? (primaryPosition.market || selected.label) : selected.label)}</strong>
                <p class="live-spotlight-summary">${esc(topSummary)}</p>
                <div class="live-spotlight-value-wrap">
                  <span>${esc(primaryPosition ? "현재 수익률" : "오늘 순손익")}</span>
                  <strong class="live-spotlight-value ${spotlightTone}">${esc(spotlightValue)}</strong>
                </div>
                <div class="live-spotlight-tags">
                  ${spotlightTags.map((item) => `<span class="live-spotlight-tag">${esc(item)}</span>`).join("")}
                </div>
              </article>
              <article class="live-session-card">
                <div class="live-session-head">
                  <div>
                    <h5>오늘 세션 요약</h5>
                    <p>${esc(todaySummaryLine)}</p>
                  </div>
                  ${pill("개인 WS", privateWsFreshness, privateWsLastTs == null ? "warn" : "good")}
                </div>
                <div class="live-session-tags">
                  ${todaySummaryTags.map((item) => `<span class="live-session-tag">${esc(item)}</span>`).join("")}
                </div>
                <div class="metric-grid">
                  ${metric("승률", fmtPct(today.win_rate_pct))}
                  ${metric("손익 확정", verifiedClosed)}
                  ${metric("미확정 종료", unverifiedClosed)}
                  ${metric("진입 대기", maybe(today.current_pending_orders_count, "0"))}
                  ${metric("청산 대기", maybe(today.current_exit_orders_count, "0"))}
                  ${metric("마지막 개인 WS", fmtCompactDateTime(privateWsLastTs))}
                </div>
              </article>
            </div>
            <div class="live-status-strip">
              ${runtimeSignals}
            </div>
            ${issueRail ? `<div class="live-inline-stack">${issueRail}</div>` : ""}
          </article>
        </section>
        <section class="live-board-grid">
          ${surfaceCard({
            title: "전체 포지션 & 청산",
            copy: positions.length > 1 ? `${positions.length}건 전체 상세` : primaryPosition ? "상단은 핵심 1건 요약, 아래는 상세" : "현재는 포지션 없이 관찰 중입니다.",
            body: positionSection,
            extraClass: "live-span-2"
          })}
          ${surfaceCard({
            title: "최근 의사결정",
            copy: intents.length ? "최근 진입/정리 판단만 짧게 보여줍니다." : "",
            body: intentSection
          })}
          ${surfaceCard({
            title: "주문 큐",
            copy: openOrders.length ? "" : "",
            body: orderSection
          })}
          ${surfaceCard({
            title: "최근 거래",
            copy: "",
            body: tradeSection,
            extraClass: "live-span-2"
          })}
        </section>
      </div>
    `;
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
    document.getElementById("ws-details").innerHTML = `<div class="dense-list">${
      [
        compactRow({
          title: "수집기 상태",
          summary: health.connected ? "WS 수집기가 정상 연결 상태입니다." : "WS 수집기가 끊겨 있습니다.",
          items: [
            compactStat("연결", boolLabel(health.connected)),
            compactStat("재연결 횟수", maybe(health.reconnect_count, "0")),
            compactStat("최근 수신", fmtDateTime(lastRxTs)),
            compactStat("fatal reason", maybe(health.fatal_reason)),
          ],
        }),
        compactRow({
          title: "누적 적재",
          summary: `최근 run ${fmtNumber(latestRun.parts, 0)} parts · ${fmtNumber(latestRun.rows_total, 0)} rows`,
          items: [
            compactStat("총 적재 행", fmtNumber((health.written_rows || {}).total, 0)),
            compactStat("trade 행", fmtNumber((health.written_rows || {}).trade, 0)),
            compactStat("orderbook 행", fmtNumber((health.written_rows || {}).orderbook, 0)),
            compactStat("총 drop 행", fmtNumber((health.dropped_rows || {}).total, 0)),
          ],
        }),
      ].join("")
    }</div>`;
  }

  function renderAll(snapshot) {
    renderMeta(snapshot);
    renderOverview(snapshot);
    renderTraining(snapshot);
    renderPaper(snapshot);
    renderLive(snapshot);
    renderWs(snapshot);
    setTab(state.activeTab, false, { scroll: false });
  }

  async function responseErrorText(response) {
    try {
      const text = await response.text();
      const detail = normalizeDisplayValue(text, 220);
      return detail && detail !== "-"
        ? `snapshot 응답 실패 (${response.status}) · ${detail}`
        : `snapshot 응답 실패 (${response.status})`;
    } catch {
      return `snapshot 응답 실패 (${response.status})`;
    }
  }

  async function refresh() {
    try {
      const response = await fetch("/api/snapshot", { cache: "no-store" });
      if (!response.ok) throw new Error(await responseErrorText(response));
      renderAll(await response.json());
      setError("");
    } catch (err) {
      setError(`실시간 새로고침 실패: ${err && err.message ? err.message : err}`);
    }
  }

  function startFallbackRefresh() {
    if (state.fallbackRefreshTimer != null) return;
    state.fallbackRefreshTimer = setInterval(refresh, 15000);
  }

  function stopFallbackRefresh() {
    if (state.fallbackRefreshTimer == null) return;
    clearInterval(state.fallbackRefreshTimer);
    state.fallbackRefreshTimer = null;
  }

  function startStream() {
    if (!("EventSource" in window)) {
      startFallbackRefresh();
      return;
    }
    if (state.stream) {
      state.stream.close();
    }
    const stream = new EventSource("/api/stream");
    state.stream = stream;
    stream.onopen = () => {
      stopFallbackRefresh();
      setError("");
    };
    const applySnapshotEvent = (event) => {
      try {
        renderAll(JSON.parse(event.data));
        setError("");
      } catch (err) {
        setError(`실시간 데이터 해석 실패: ${err && err.message ? err.message : err}`);
      }
    };
    stream.onmessage = applySnapshotEvent;
    stream.addEventListener("snapshot", applySnapshotEvent);
    stream.onerror = () => {
      if (state.stream === stream) {
        stream.close();
        state.stream = null;
      }
      startFallbackRefresh();
      setError("실시간 연결이 불안정해 보조 새로고침으로 전환했습니다.");
    };
  }

  bindLayout();
  bindTabs();
  renderAll(INITIAL_SNAPSHOT);
  setTab(state.activeTab, false, { scroll: false });
  refresh();
  startStream();
})();

