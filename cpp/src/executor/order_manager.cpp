#include "order_manager.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <utility>

#include "upbit/number_string.h"

namespace autobot::executor {

namespace {

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

double ParseNumber(const std::string& raw) {
  if (raw.empty()) {
    return 0.0;
  }
  try {
    return std::stod(raw);
  } catch (...) {
    return 0.0;
  }
}

bool IsClosedOrderState(const std::string& state) {
  const std::string lowered = ToLower(state);
  return lowered == "done" || lowered == "cancel" || lowered == "cancelled" || lowered == "reject";
}

bool IsOpenOrderState(const std::string& state) {
  const std::string lowered = ToLower(state);
  return lowered == "wait" || lowered == "watch";
}

bool IsPositiveNumberString(const std::string& raw) {
  if (raw.empty()) {
    return false;
  }
  try {
    return std::stod(raw) > 0.0;
  } catch (...) {
    return false;
  }
}

int ParseIntEnv(const char* key, int fallback, int minimum) {
  const char* raw = std::getenv(key);
  if (raw == nullptr) {
    return std::max(fallback, minimum);
  }
  try {
    const int parsed = std::stoi(raw);
    return std::max(parsed, minimum);
  } catch (...) {
    return std::max(fallback, minimum);
  }
}

bool ParseBoolEnv(const char* key, bool fallback) {
  const char* raw = std::getenv(key);
  if (raw == nullptr) {
    return fallback;
  }
  std::string value = ToLower(raw);
  value.erase(std::remove_if(value.begin(), value.end(), [](unsigned char ch) { return std::isspace(ch); }), value.end());
  if (value == "1" || value == "true" || value == "yes" || value == "on") {
    return true;
  }
  if (value == "0" || value == "false" || value == "no" || value == "off") {
    return false;
  }
  return fallback;
}

std::string ParseStringEnv(const char* key, const std::string& fallback) {
  const char* raw = std::getenv(key);
  if (raw == nullptr) {
    return fallback;
  }
  std::string value(raw);
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) { return !std::isspace(ch); }));
  value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(), value.end());
  if (value.empty()) {
    return fallback;
  }
  return value;
}

}  // namespace

OrderManager::OrderManager(
    UpbitRestClient* rest_client,
    std::function<std::unique_ptr<upbit::UpbitPrivateWsClient>(
        const upbit::WsPrivateClientOptions&)>
        private_ws_factory)
    : rest_client_(rest_client),
      private_ws_factory_(std::move(private_ws_factory)) {
  order_timeout_sec_ = ParseIntEnv("AUTOBOT_EXECUTOR_ORDER_TIMEOUT_SEC", 0, 0);
  timeout_replace_enabled_ =
      ParseBoolEnv("AUTOBOT_EXECUTOR_ORDER_TIMEOUT_REPLACE_ENABLED", false);
  StartPrivateWsIfNeeded();
  StartPollingIfNeeded();
}

OrderManager::~OrderManager() {
  StopPrivateWs();
  StopPolling();
}

ManagedResult OrderManager::SubmitIntent(const ManagedIntent& intent) {
  if (intent.identifier.empty()) {
    ManagedResult rejected;
    rejected.accepted = false;
    rejected.reason = "identifier is required";
    rejected.intent_id = intent.intent_id;
    return rejected;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = by_identifier_.find(intent.identifier);
    if (found != by_identifier_.end()) {
      return found->second;
    }
  }

  UpbitSubmitRequest submit_request;
  submit_request.intent_id = intent.intent_id;
  submit_request.identifier = intent.identifier;
  submit_request.market = intent.market;
  submit_request.side = intent.side;
  submit_request.price = intent.price;
  submit_request.volume = intent.volume;
  submit_request.tif = intent.tif;
  submit_request.meta_json = intent.meta_json;

  const UpbitSubmitResult submit = rest_client_->SubmitLimitOrder(submit_request);

  ManagedResult result;
  result.accepted = submit.accepted;
  result.reason = submit.reason;
  result.upbit_uuid = submit.upbit_uuid;
  result.identifier = submit.identifier.empty() ? intent.identifier : submit.identifier;
  result.intent_id = intent.intent_id;

  const std::string price_str = upbit::FormatPriceString(intent.price, 0.0, 16);
  const std::string volume_str = upbit::FormatVolumeString(intent.volume, 16);

  {
    std::lock_guard<std::mutex> lock(mutex_);
    by_identifier_[result.identifier] = result;
    if (submit.accepted) {
      order_by_identifier_[result.identifier] = {
          {"event_name", "ORDER_ACCEPTED"},
          {"uuid", submit.upbit_uuid},
          {"upbit_uuid", submit.upbit_uuid},
          {"identifier", result.identifier},
          {"intent_id", intent.intent_id},
          {"market", intent.market},
          {"side", intent.side},
          {"ord_type", intent.ord_type},
          {"state", submit.state.empty() ? "wait" : submit.state},
          {"price_str", price_str},
          {"volume_str", volume_str},
          {"executed_volume_str", "0"},
          {"remaining_volume_str", volume_str},
          {"lifecycle_state", "open"},
          {"replace_attempt_count", 0},
          {"mode", rest_client_->ModeName()},
      };
    }
  }

  if (submit.accepted) {
    nlohmann::json payload = {
        {"event_name", "ORDER_ACCEPTED"},
        {"intent_id", intent.intent_id},
        {"identifier", result.identifier},
        {"uuid", submit.upbit_uuid},
        {"upbit_uuid", submit.upbit_uuid},
        {"market", intent.market},
        {"side", intent.side},
        {"ord_type", intent.ord_type},
        {"price_str", price_str},
        {"volume_str", volume_str},
        {"mode", rest_client_->ModeName()},
        {"ts_ms", NowMs()},
        {"state", submit.state.empty() ? "wait" : submit.state},
        {"executed_volume", "0"},
        {"remaining_volume_str", volume_str},
        {"lifecycle_state", "open"},
        {"replace_attempt_count", 0},
        {"volume", volume_str},
        {"price", price_str},
    };
    if (submit.remaining_req_sec >= 0) {
      payload["remaining_req"] = {
          {"group", submit.remaining_req_group},
          {"sec", submit.remaining_req_sec},
      };
    }
    PushEvent("ORDER_UPDATE", NowMs(), payload);
    TrackOrder(intent, result);
    return result;
  }

  nlohmann::json error_payload = {
      {"event_name", "ERROR"},
      {"where", "order_manager.submit"},
      {"http_status", submit.http_status},
      {"upbit_error_name", submit.error_name},
      {"upbit_error_message", submit.reason},
      {"breaker_state", submit.breaker_state},
      {"identifier", result.identifier},
      {"upbit_uuid", result.upbit_uuid},
      {"operator_intervention_required", submit.operator_intervention_required},
      {"ts_ms", NowMs()},
  };
  PushEvent("ERROR", NowMs(), error_payload);
  return result;
}

ManagedResult OrderManager::Cancel(const std::string& upbit_uuid, const std::string& identifier) {
  if (upbit_uuid.empty() && identifier.empty()) {
    ManagedResult rejected;
    rejected.accepted = false;
    rejected.reason = "upbit_uuid or identifier is required";
    return rejected;
  }

  UpbitCancelRequest cancel_request;
  cancel_request.upbit_uuid = upbit_uuid;
  cancel_request.identifier = identifier;
  const UpbitCancelResult cancel = rest_client_->CancelOrder(cancel_request);

  const std::string resolved_identifier = identifier.empty() ? cancel.identifier : identifier;
  ManagedResult result;
  result.accepted = cancel.accepted;
  result.reason = cancel.reason;
  result.upbit_uuid = cancel.upbit_uuid;
  result.identifier = resolved_identifier;
  const std::string cancel_state =
      cancel.state.empty() ? (cancel.accepted ? "cancel" : "cancel_reject") : cancel.state;
  std::string market;
  std::string side;
  std::string ord_type;
  std::string price_str;
  std::string volume_str;
  std::string executed_volume_str;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!resolved_identifier.empty()) {
      auto found = by_identifier_.find(resolved_identifier);
      if (found != by_identifier_.end()) {
        result.intent_id = found->second.intent_id;
        found->second.upbit_uuid = cancel.upbit_uuid;
        found->second.reason = cancel.reason;
      }
      auto order_it = order_by_identifier_.find(resolved_identifier);
      if (order_it != order_by_identifier_.end()) {
        order_it->second["state"] = cancel_state;
        order_it->second["lifecycle_state"] = "cancel";
        market = order_it->second.value("market", "");
        side = order_it->second.value("side", "");
        ord_type = order_it->second.value("ord_type", "");
        price_str = order_it->second.value("price_str", "");
        volume_str = order_it->second.value("volume_str", "");
        executed_volume_str = order_it->second.value("executed_volume_str", "0");
      }
      tracked_orders_.erase(TrackingKey(cancel.upbit_uuid, resolved_identifier));
    }
  }

  nlohmann::json payload = {
      {"event_name", "CANCEL_RESULT"},
      {"uuid", cancel.upbit_uuid},
      {"upbit_uuid", cancel.upbit_uuid},
      {"identifier", resolved_identifier},
      {"ok", cancel.accepted},
      {"reason", cancel.reason},
      {"state", cancel_state},
      {"market", market},
      {"side", side},
      {"ord_type", ord_type},
      {"price", price_str},
      {"volume", volume_str},
      {"executed_volume", executed_volume_str.empty() ? "0" : executed_volume_str},
      {"ts_ms", NowMs()},
      {"mode", rest_client_->ModeName()},
  };
  if (cancel.remaining_req_sec >= 0) {
    payload["remaining_req"] = {
        {"group", cancel.remaining_req_group},
        {"sec", cancel.remaining_req_sec},
    };
  }

  PushEvent("ORDER_UPDATE", NowMs(), payload);
  if (!cancel.accepted) {
    PushEvent(
        "ERROR",
        NowMs(),
        {
            {"event_name", "ERROR"},
            {"where", "order_manager.cancel"},
            {"http_status", cancel.http_status},
            {"upbit_error_name", cancel.error_name},
            {"upbit_error_message", cancel.reason},
            {"breaker_state", cancel.breaker_state},
            {"identifier", result.identifier},
            {"upbit_uuid", result.upbit_uuid},
            {"ts_ms", NowMs()},
        });
  }
  return result;
}

ManagedReplaceResult OrderManager::ReplaceOrder(const ManagedReplaceRequest& request) {
  ManagedReplaceResult result;

  const std::string prev_order_uuid = request.prev_order_uuid;
  std::string prev_order_identifier = request.prev_order_identifier;
  const std::string new_identifier = request.new_identifier;
  const std::string new_price_str = request.new_price_str;
  const std::string new_volume_str = request.new_volume_str;
  const std::string new_time_in_force = request.new_time_in_force;

  if (prev_order_uuid.empty() && prev_order_identifier.empty()) {
    result.reason = "prev_order_uuid or prev_order_identifier is required";
    return result;
  }
  if (!prev_order_uuid.empty() && !prev_order_identifier.empty()) {
    prev_order_identifier.clear();
  }
  if (new_identifier.empty()) {
    result.reason = "new_identifier is required";
    return result;
  }
  if (!IsPositiveNumberString(new_price_str)) {
    result.reason = "new_price_str must be a positive number string";
    return result;
  }
  if (new_volume_str.empty()) {
    result.reason = "new_volume_str is required";
    return result;
  }
  if (ToLower(new_volume_str) != "remain_only" && !IsPositiveNumberString(new_volume_str)) {
    result.reason = "new_volume_str must be a positive number string or remain_only";
    return result;
  }

  UpbitReplaceRequest replace_request;
  replace_request.intent_id = request.intent_id;
  replace_request.prev_order_uuid = prev_order_uuid;
  replace_request.prev_order_identifier = prev_order_identifier;
  replace_request.new_identifier = new_identifier;
  replace_request.new_price_str = new_price_str;
  replace_request.new_volume_str = new_volume_str;
  replace_request.new_time_in_force = new_time_in_force;
  const UpbitReplaceResult replace = rest_client_->ReplaceOrder(replace_request);

  result.accepted = replace.accepted;
  result.reason = replace.reason;
  result.cancelled_order_uuid = replace.cancelled_order_uuid;
  result.new_order_uuid = replace.new_order_uuid;
  result.new_identifier = replace.new_identifier.empty() ? new_identifier : replace.new_identifier;

  if (!replace.accepted) {
    PushEvent(
        "ERROR",
        NowMs(),
        {
            {"event_name", "ERROR"},
            {"where", "order_manager.replace"},
            {"http_status", replace.http_status},
            {"upbit_error_name", replace.error_name},
            {"upbit_error_message", replace.reason},
            {"breaker_state", replace.breaker_state},
            {"identifier", result.new_identifier},
            {"upbit_uuid", result.new_order_uuid},
            {"ts_ms", NowMs()},
        });
    return result;
  }

  std::string resolved_prev_identifier = prev_order_identifier;
  std::string market;
  std::string side;
  std::string ord_type;
  std::string prev_volume_str;
  int replace_attempt_count = 1;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (resolved_prev_identifier.empty() && !prev_order_uuid.empty()) {
      const auto found_by_uuid = tracked_orders_.find(TrackingKey(prev_order_uuid, ""));
      if (found_by_uuid != tracked_orders_.end()) {
        resolved_prev_identifier = found_by_uuid->second.identifier;
      } else {
        for (const auto& [_, candidate] : tracked_orders_) {
          if (candidate.upbit_uuid == prev_order_uuid && !candidate.identifier.empty()) {
            resolved_prev_identifier = candidate.identifier;
            break;
          }
        }
      }
    }

    if (!resolved_prev_identifier.empty()) {
      auto prev_order_it = order_by_identifier_.find(resolved_prev_identifier);
      if (prev_order_it != order_by_identifier_.end()) {
        market = prev_order_it->second.value("market", "");
        side = prev_order_it->second.value("side", "");
        ord_type = prev_order_it->second.value("ord_type", "limit");
        prev_volume_str = prev_order_it->second.value("volume_str", "");
        prev_order_it->second["state"] = "cancel";
        prev_order_it->second["lifecycle_state"] = "replaced";
      }
    }

    const std::string prev_tracking_key = TrackingKey(prev_order_uuid, resolved_prev_identifier);
    if (!prev_tracking_key.empty()) {
      auto tracked_it = tracked_orders_.find(prev_tracking_key);
      if (tracked_it != tracked_orders_.end()) {
        replace_attempt_count = std::max(tracked_it->second.replace_attempt_count + 1, 1);
        if (market.empty()) {
          market = tracked_it->second.market;
        }
        if (side.empty()) {
          side = tracked_it->second.side;
        }
        if (ord_type.empty()) {
          ord_type = tracked_it->second.ord_type;
        }
        if (prev_volume_str.empty()) {
          prev_volume_str = tracked_it->second.volume_str;
        }
      }
      tracked_orders_.erase(prev_tracking_key);
    }

    ManagedResult& new_result = by_identifier_[result.new_identifier];
    new_result.accepted = true;
    new_result.reason = replace.reason;
    new_result.upbit_uuid = result.new_order_uuid;
    new_result.identifier = result.new_identifier;
    new_result.intent_id = request.intent_id;

    nlohmann::json& row = order_by_identifier_[result.new_identifier];
    if (!row.is_object()) {
      row = nlohmann::json::object();
    }
    row["event_name"] = "ORDER_STATE";
    row["uuid"] = result.new_order_uuid;
    row["upbit_uuid"] = result.new_order_uuid;
    row["identifier"] = result.new_identifier;
    row["market"] = market;
    row["side"] = side;
    row["ord_type"] = ord_type.empty() ? "limit" : ord_type;
    row["state"] = "wait";
    row["price_str"] = new_price_str;
    row["volume_str"] = ToLower(new_volume_str) == "remain_only" ? prev_volume_str : new_volume_str;
    row["executed_volume_str"] = "0";
    row["remaining_volume_str"] = row["volume_str"];
    row["lifecycle_state"] = "open";
    row["replace_attempt_count"] = replace_attempt_count;
    row["source"] = "replace";
    row["mode"] = rest_client_->ModeName();

    if (!result.new_order_uuid.empty()) {
      const std::string new_tracking_key = TrackingKey(result.new_order_uuid, result.new_identifier);
      TrackedOrder& tracked = tracked_orders_[new_tracking_key];
      tracked.upbit_uuid = result.new_order_uuid;
      tracked.identifier = result.new_identifier;
      tracked.market = market;
      tracked.side = side;
      tracked.ord_type = row.value("ord_type", "limit");
      tracked.price_str = new_price_str;
      tracked.volume_str = row.value("volume_str", "");
      tracked.state = "wait";
      tracked.executed_volume_str = "0";
      tracked.remaining_volume_str = tracked.volume_str;
      tracked.last_executed_volume = 0.0;
      tracked.replace_attempt_count = replace_attempt_count;
      tracked.timeout_emitted = false;
      tracked.cancel_requested = false;
      tracked.created_ts_ms = NowMs();
      tracked.last_state_ts_ms = tracked.created_ts_ms;
      tracked.last_fill_ts_ms = 0;
    }
  }

  nlohmann::json payload = {
      {"event_name", "ORDER_REPLACED"},
      {"intent_id", request.intent_id},
      {"prev_uuid", result.cancelled_order_uuid.empty() ? prev_order_uuid : result.cancelled_order_uuid},
      {"prev_identifier", resolved_prev_identifier},
      {"new_uuid", result.new_order_uuid.empty() ? nlohmann::json(nullptr) : nlohmann::json(result.new_order_uuid)},
      {"new_identifier", result.new_identifier},
      {"new_price_str", new_price_str},
      {"new_volume_str", new_volume_str},
      {"replace_attempt_count", replace_attempt_count},
      {"reason", result.reason},
      {"ts_ms", NowMs()},
      {"mode", rest_client_->ModeName()},
  };
  if (replace.remaining_req_sec >= 0) {
    payload["remaining_req"] = {
        {"group", replace.remaining_req_group},
        {"sec", replace.remaining_req_sec},
    };
  }
  PushEvent("ORDER_UPDATE", NowMs(), std::move(payload));
  return result;
}

bool OrderManager::PopEvent(ManagedEvent* out, std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (!cv_.wait_for(lock, timeout, [this] { return !events_.empty(); })) {
    return false;
  }
  *out = events_.front();
  events_.pop_front();
  return true;
}

nlohmann::json OrderManager::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);

  nlohmann::json orders = nlohmann::json::array();
  for (const auto& [identifier, order] : order_by_identifier_) {
    nlohmann::json item = order;
    item["identifier"] = identifier;
    orders.push_back(item);
  }

  nlohmann::json intents = nlohmann::json::array();
  for (const auto& [identifier, result] : by_identifier_) {
    intents.push_back(
        {{"identifier", identifier},
         {"intent_id", result.intent_id},
         {"accepted", result.accepted},
         {"reason", result.reason},
         {"upbit_uuid", result.upbit_uuid}});
  }

  nlohmann::json ws_private = {
      {"enabled", private_ws_enabled_},
      {"connected", private_ws_connected_},
      {"poll_interval_rest_only_ms", poll_interval_rest_only_ms_},
      {"poll_interval_ws_connected_ms", poll_interval_ws_connected_ms_},
      {"poll_interval_ws_degraded_ms", poll_interval_ws_degraded_ms_},
      {"current_poll_interval_ms", CurrentPollingIntervalLocked().count()},
      {"order_timeout_sec", order_timeout_sec_},
      {"order_timeout_replace_enabled", timeout_replace_enabled_},
  };
  if (private_ws_client_ != nullptr) {
    const auto stats = private_ws_client_->Stats();
    ws_private["reconnect_count"] = stats.reconnect_count;
    ws_private["received_events"] = stats.received_events;
    ws_private["last_event_ts_ms"] = stats.last_event_ts_ms;
    ws_private["last_connect_ts_ms"] = stats.last_connect_ts_ms;
    ws_private["last_disconnect_ts_ms"] = stats.last_disconnect_ts_ms;
    ws_private["last_rx_ts_ms"] = stats.last_rx_ts_ms;
    ws_private["last_tx_ts_ms"] = stats.last_tx_ts_ms;
    ws_private["last_ping_ts_ms"] = stats.last_ping_ts_ms;
    ws_private["last_pong_ts_ms"] = stats.last_pong_ts_ms;
    ws_private["ping_sent_count"] = stats.ping_sent_count;
    ws_private["pong_rx_count"] = stats.pong_rx_count;
    ws_private["stale_disconnect_count"] = stats.stale_disconnect_count;
    ws_private["keepalive_mode"] = stats.keepalive_mode;
    ws_private["keepalive_ping_interval_sec"] = stats.keepalive_ping_interval_sec;
    ws_private["keepalive_pong_grace_sec"] = stats.keepalive_pong_grace_sec;
    ws_private["keepalive_ping_on_connect"] = stats.keepalive_ping_on_connect;
    ws_private["keepalive_force_reconnect_on_stale"] = stats.keepalive_force_reconnect_on_stale;
    ws_private["keepalive_up_status_log"] = stats.keepalive_up_status_log;
    ws_private["last_disconnect_reason"] = stats.last_disconnect_reason;
  }
  return {{"orders", orders}, {"intents", intents}, {"ws_private", ws_private}};
}

void OrderManager::PushEvent(const std::string& event_type, std::int64_t ts_ms, nlohmann::json payload) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ManagedEvent event;
    event.event_type = event_type;
    event.ts_ms = ts_ms;
    event.payload = std::move(payload);
    events_.push_back(std::move(event));
  }
  cv_.notify_one();
}

void OrderManager::StartPollingIfNeeded() {
  if (rest_client_ == nullptr || rest_client_->IsOrderTestMode()) {
    return;
  }
  const int rest_only_ms =
      ParseIntEnv("AUTOBOT_EXECUTOR_POLL_INTERVAL_REST_ONLY_MS", 1500, 200);
  const int ws_connected_sec =
      ParseIntEnv("AUTOBOT_EXECUTOR_POLL_INTERVAL_WS_CONNECTED_SEC", 180, 1);
  const int ws_degraded_sec =
      ParseIntEnv("AUTOBOT_EXECUTOR_POLL_INTERVAL_WS_DEGRADED_SEC", 60, 1);

  {
    std::lock_guard<std::mutex> lock(mutex_);
    poll_interval_rest_only_ms_ = rest_only_ms;
    poll_interval_ws_connected_ms_ = ws_connected_sec * 1000;
    poll_interval_ws_degraded_ms_ = ws_degraded_sec * 1000;
    if (poller_thread_.joinable()) {
      return;
    }
    stop_poller_ = false;
  }
  poller_thread_ = std::thread(&OrderManager::PollLoop, this);
}

void OrderManager::StartPrivateWsIfNeeded() {
  if (rest_client_ == nullptr || rest_client_->IsOrderTestMode()) {
    return;
  }
  if (!rest_client_->PrivateWsEnabled()) {
    return;
  }

  upbit::WsPrivateClientOptions options;
  options.url = rest_client_->PrivateWsUrl();
  options.subscribe_my_order = ParseBoolEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLE_MYORDER", true);
  options.subscribe_my_asset = ParseBoolEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLE_MYASSET", true);
  options.my_order_codes = rest_client_->PrivateWsOrderCodes();
  options.subscribe_format = ParseStringEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_FORMAT", "DEFAULT");
  options.format_fallback_to_default_once =
      ParseBoolEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_FORMAT_FALLBACK_ONCE", true);
  options.connect_rps = ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_CONNECT_RPS", 5, 1);
  options.message_rps = ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_MESSAGE_RPS", 5, 1);
  options.message_rpm = ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_MESSAGE_RPM", 100, 1);
  options.connect_timeout_ms = ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_CONNECT_TIMEOUT_MS", 3000, 100);
  options.read_timeout_ms = ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_READ_TIMEOUT_MS", 1000, 100);
  options.write_timeout_ms = ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_WRITE_TIMEOUT_MS", 3000, 100);
  options.keepalive_mode = ParseStringEnv(
      "AUTOBOT_EXECUTOR_PRIVATE_WS_KEEPALIVE_MODE",
      ParseStringEnv("PRIVATE_WS_KEEPALIVE_MODE", "message"));
  options.ping_on_connect = ParseBoolEnv(
      "AUTOBOT_EXECUTOR_PRIVATE_WS_PING_ON_CONNECT",
      ParseBoolEnv("PRIVATE_WS_PING_ON_CONNECT", true));
  options.ping_interval_sec = ParseIntEnv(
      "AUTOBOT_EXECUTOR_PRIVATE_WS_PING_INTERVAL_SEC",
      ParseIntEnv("PRIVATE_WS_PING_INTERVAL_SEC", 60, 1),
      1);
  options.pong_grace_sec = ParseIntEnv(
      "AUTOBOT_EXECUTOR_PRIVATE_WS_PONG_GRACE_SEC",
      ParseIntEnv("PRIVATE_WS_PONG_GRACE_SEC", 20, 1),
      1);
  options.stale_rx_threshold_sec = ParseIntEnv(
      "AUTOBOT_EXECUTOR_PRIVATE_WS_STALE_RX_THRESHOLD_SEC",
      110,
      1);
  options.force_reconnect_on_stale = ParseBoolEnv(
      "AUTOBOT_EXECUTOR_PRIVATE_WS_FORCE_RECONNECT_ON_STALE",
      ParseBoolEnv("PRIVATE_WS_FORCE_RECONNECT_ON_STALE", true));
  options.up_status_log = ParseBoolEnv(
      "AUTOBOT_EXECUTOR_PRIVATE_WS_UP_STATUS_LOG",
      ParseBoolEnv("PRIVATE_WS_UP_STATUS_LOG", false));
  options.idle_timeout_sec = ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_IDLE_TIMEOUT_SEC", 125, 5);
  options.reconnect_enabled = ParseBoolEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_RECONNECT_ENABLED", true);
  options.reconnect_base_delay_ms =
      ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_RECONNECT_BASE_MS", 1000, 100);
  options.reconnect_max_delay_ms =
      ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_RECONNECT_MAX_MS", 15000, 1000);
  options.reconnect_jitter_ms =
      ParseIntEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_RECONNECT_JITTER_MS", 300, 0);

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (private_ws_thread_.joinable()) {
      return;
    }
    private_ws_enabled_ = true;
    private_ws_connected_ = false;
    private_ws_up_status_log_ = options.up_status_log;
  }

  if (private_ws_factory_) {
    private_ws_client_ = private_ws_factory_(options);
  } else {
    private_ws_client_ = std::make_unique<upbit::UpbitPrivateWsClient>(options);
  }
  if (private_ws_client_ == nullptr) {
    std::lock_guard<std::mutex> lock(mutex_);
    private_ws_enabled_ = false;
    private_ws_connected_ = false;
    return;
  }
  stop_private_ws_.store(false);
  BootstrapAssetSnapshot();
  private_ws_thread_ = std::thread(&OrderManager::PrivateWsLoop, this);
}

void OrderManager::PrivateWsLoop() {
  if (private_ws_client_ == nullptr || rest_client_ == nullptr) {
    return;
  }

  upbit::UpbitPrivateWsClient::Callbacks callbacks;
  callbacks.on_connected = [this]() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      private_ws_connected_ = true;
    }
    cv_.notify_all();
  };
  callbacks.on_disconnected = [this](const std::string& reason) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      private_ws_connected_ = false;
    }
    cv_.notify_all();
    if (!reason.empty() && reason != "stopped") {
      PushEvent(
          "ERROR",
          NowMs(),
          {
              {"event_name", "ERROR"},
              {"where", "order_manager.private_ws.disconnect"},
              {"upbit_error_message", reason},
              {"ts_ms", NowMs()},
          });
    }
  };
  callbacks.on_error = [this](const std::string& reason) {
    PushEvent(
        "ERROR",
        NowMs(),
        {
            {"event_name", "ERROR"},
            {"where", "order_manager.private_ws"},
            {"upbit_error_message", reason},
            {"ts_ms", NowMs()},
        });
  };
  callbacks.on_my_order = [this](const upbit::WsMyOrderEvent& event) { HandleWsOrderEvent(event); };
  callbacks.on_my_asset = [this](const upbit::WsMyAssetEvent& event) { HandleWsAssetEvent(event); };
  callbacks.on_health_up = [this](const upbit::WsHealthUpEvent& event) {
    if (!private_ws_up_status_log_) {
      return;
    }
    PushEvent(
        "HEALTH",
        NowMs(),
        {
            {"event_name", "WS_PRIVATE_UP"},
            {"status", event.status},
            {"source", "ws_private"},
            {"updated_ts_ms", event.ts_ms > 0 ? event.ts_ms : NowMs()},
            {"ts_ms", NowMs()},
        });
  };

  private_ws_client_->Run(
      [this]() -> std::string {
        if (rest_client_ == nullptr) {
          return "";
        }
        return rest_client_->PrivateWsAuthorizationHeader();
      },
      &stop_private_ws_,
      callbacks);
}

void OrderManager::BootstrapAssetSnapshot() {
  if (rest_client_ == nullptr || rest_client_->IsOrderTestMode()) {
    return;
  }
  const UpbitAccountsSnapshotResult snapshot = rest_client_->GetAccountsSnapshot();
  if (!snapshot.ok) {
    PushEvent(
        "ERROR",
        NowMs(),
        {
            {"event_name", "ERROR"},
            {"where", "order_manager.accounts_snapshot"},
            {"http_status", snapshot.http_status},
            {"upbit_error_name", snapshot.error_name},
            {"upbit_error_message", snapshot.reason},
            {"breaker_state", snapshot.breaker_state},
            {"ts_ms", NowMs()},
        });
    return;
  }

  for (const auto& account : snapshot.accounts) {
    nlohmann::json payload = {
        {"event_name", "ASSET"},
        {"currency", account.currency},
        {"balance", account.balance_str.empty() ? "0" : account.balance_str},
        {"locked", account.locked_str.empty() ? "0" : account.locked_str},
        {"avg_buy_price", account.avg_buy_price_str},
        {"source", "rest_bootstrap"},
        {"snapshot", true},
        {"ts_ms", NowMs()},
        {"mode", rest_client_->ModeName()},
    };
    if (snapshot.remaining_req_sec >= 0) {
      payload["remaining_req"] = {
          {"group", snapshot.remaining_req_group},
          {"sec", snapshot.remaining_req_sec},
      };
    }
    PushEvent("ASSET", NowMs(), payload);
  }
}

void OrderManager::StopPrivateWs() {
  stop_private_ws_.store(true);
  cv_.notify_all();
  if (private_ws_thread_.joinable()) {
    private_ws_thread_.join();
  }
  std::lock_guard<std::mutex> lock(mutex_);
  private_ws_connected_ = false;
}

void OrderManager::StopPolling() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_poller_ = true;
  }
  cv_.notify_all();
  if (poller_thread_.joinable()) {
    poller_thread_.join();
  }
}

void OrderManager::HandleWsOrderEvent(const upbit::WsMyOrderEvent& event) {
  const std::string identifier = event.identifier;
  const std::string upbit_uuid = event.uuid;
  const std::string market = event.market;
  const std::string side = event.side;
  const std::string ord_type = event.ord_type;
  const std::string state = event.state;
  const std::string price_str = event.price_str;
  const std::string volume_str = event.volume_str;
  const std::string executed_volume_str =
      event.executed_volume_str.empty() ? "0" : event.executed_volume_str;
  const std::string remaining_volume_str = event.remaining_volume_str;
  const std::string avg_price_str = event.avg_price_str;
  const double executed_volume = ParseNumber(executed_volume_str);
  const std::int64_t now_ms = NowMs();

  bool emit_fill = false;
  double fill_volume = 0.0;
  int replace_attempt_count = 0;
  std::string lifecycle_state = ComputeLifecycleState(state, executed_volume_str, remaining_volume_str);
  const std::string tracking_key = TrackingKey(upbit_uuid, identifier);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    TrackedOrder* tracked_ptr = nullptr;
    if (!tracking_key.empty()) {
      tracked_ptr = &tracked_orders_[tracking_key];
    }

    if (tracked_ptr != nullptr) {
      const double prev_executed =
          tracked_ptr->last_executed_volume < 0.0 ? 0.0 : tracked_ptr->last_executed_volume;
      if (executed_volume > prev_executed + 1e-12) {
        emit_fill = true;
        fill_volume = executed_volume - prev_executed;
        tracked_ptr->last_fill_ts_ms = now_ms;
      }
      tracked_ptr->last_executed_volume = executed_volume;
      tracked_ptr->state = state;
      tracked_ptr->executed_volume_str = executed_volume_str;
      tracked_ptr->remaining_volume_str = remaining_volume_str;
      tracked_ptr->last_state_ts_ms = now_ms;
      tracked_ptr->timeout_emitted = false;
      tracked_ptr->cancel_requested = false;
      if (!upbit_uuid.empty()) {
        tracked_ptr->upbit_uuid = upbit_uuid;
      }
      if (!identifier.empty()) {
        tracked_ptr->identifier = identifier;
      }
      if (!market.empty()) {
        tracked_ptr->market = market;
      }
      if (!side.empty()) {
        tracked_ptr->side = side;
      }
      if (!ord_type.empty()) {
        tracked_ptr->ord_type = ord_type;
      }
      if (!price_str.empty()) {
        tracked_ptr->price_str = price_str;
      }
      if (!volume_str.empty()) {
        tracked_ptr->volume_str = volume_str;
      }
      replace_attempt_count = tracked_ptr->replace_attempt_count;
    }

    if (!identifier.empty()) {
      nlohmann::json& row = order_by_identifier_[identifier];
      if (!row.is_object()) {
        row = nlohmann::json::object();
      }
      row["event_name"] = "ORDER_STATE";
      row["uuid"] = upbit_uuid;
      row["upbit_uuid"] = upbit_uuid;
      row["identifier"] = identifier;
      row["market"] = market;
      row["side"] = side;
      row["ord_type"] = ord_type;
      row["state"] = state;
      row["price_str"] = price_str;
      row["volume_str"] = volume_str;
      row["executed_volume_str"] = executed_volume_str;
      row["remaining_volume_str"] = remaining_volume_str;
      row["avg_price_str"] = avg_price_str.empty() ? nlohmann::json(nullptr) : nlohmann::json(avg_price_str);
      row["lifecycle_state"] = lifecycle_state;
      row["replace_attempt_count"] = replace_attempt_count;
      row["source"] = "ws_private";
      row["mode"] = rest_client_->ModeName();
    }

    if (IsClosedOrderState(state) && !tracking_key.empty()) {
      tracked_orders_.erase(tracking_key);
    }
  }

  PushEvent(
      "ORDER_UPDATE",
      now_ms,
      {
          {"event_name", "ORDER_STATE"},
          {"uuid", upbit_uuid},
          {"upbit_uuid", upbit_uuid},
          {"identifier", identifier},
          {"market", market},
          {"side", side},
          {"ord_type", ord_type},
          {"state", state},
          {"price_str", price_str},
          {"volume_str", volume_str},
          {"executed_volume_str", executed_volume_str},
          {"remaining_volume_str", remaining_volume_str},
          {"avg_price_str", avg_price_str.empty() ? nlohmann::json(nullptr) : nlohmann::json(avg_price_str)},
          {"lifecycle_state", lifecycle_state},
          {"replace_attempt_count", replace_attempt_count},
          {"price", price_str},
          {"volume", volume_str},
          {"executed_volume", executed_volume_str},
          {"updated_ts_ms", event.ts_ms > 0 ? event.ts_ms : now_ms},
          {"ts_ms", now_ms},
          {"source", "ws_private"},
          {"mode", rest_client_->ModeName()},
      });

  if (!emit_fill) {
    return;
  }

  const std::string fill_volume_str = upbit::FormatVolumeString(fill_volume, 16);
  PushEvent(
      "FILL",
      now_ms,
      {
          {"event_name", "FILL"},
          {"uuid", upbit_uuid},
          {"upbit_uuid", upbit_uuid},
          {"identifier", identifier},
          {"market", market},
          {"price_str", avg_price_str.empty() ? price_str : avg_price_str},
          {"volume_str", fill_volume_str},
          {"fee_str", nlohmann::json(nullptr)},
          {"ts_ms", event.ts_ms > 0 ? event.ts_ms : now_ms},
          {"price", avg_price_str.empty() ? price_str : avg_price_str},
          {"volume", fill_volume_str},
          {"executed_volume", executed_volume_str},
          {"source", "ws_private"},
      });
}

void OrderManager::HandleWsAssetEvent(const upbit::WsMyAssetEvent& event) {
  PushEvent(
      "ASSET",
      NowMs(),
      {
          {"event_name", "ASSET"},
          {"currency", event.currency},
          {"balance", event.balance_str.empty() ? "0" : event.balance_str},
          {"locked", event.locked_str.empty() ? "0" : event.locked_str},
          {"avg_buy_price", event.avg_buy_price_str},
          {"source", "ws_private"},
          {"snapshot", false},
          {"ts_ms", event.ts_ms > 0 ? event.ts_ms : NowMs()},
          {"mode", rest_client_->ModeName()},
      });
}

std::string OrderManager::ComputeLifecycleState(
    const std::string& state,
    const std::string& executed_volume_str,
    const std::string& remaining_volume_str) const {
  const std::string lowered_state = ToLower(state);
  if (IsClosedOrderState(lowered_state)) {
    return lowered_state;
  }

  const double executed = ParseNumber(executed_volume_str);
  const double remaining = ParseNumber(remaining_volume_str);
  if (executed > 0.0 && remaining > 0.0) {
    return "partial";
  }
  if (executed > 0.0 && remaining <= 0.0) {
    return "filled";
  }
  if (IsOpenOrderState(lowered_state)) {
    return "open";
  }
  return lowered_state.empty() ? "unknown" : lowered_state;
}

void OrderManager::HandleOrderTimeout(const TrackedOrder& tracked, std::int64_t now_ms) {
  if (tracked.cancel_requested) {
    return;
  }

  const double elapsed_sec =
      std::max(static_cast<double>(now_ms - tracked.last_state_ts_ms) / 1000.0, 0.0);
  PushEvent(
      "ORDER_UPDATE",
      now_ms,
      {
          {"event_name", "ORDER_TIMEOUT"},
          {"uuid", tracked.upbit_uuid},
          {"upbit_uuid", tracked.upbit_uuid},
          {"identifier", tracked.identifier},
          {"market", tracked.market},
          {"state", tracked.state},
          {"elapsed_sec", elapsed_sec},
          {"replace_enabled", timeout_replace_enabled_},
          {"ts_ms", now_ms},
          {"mode", rest_client_->ModeName()},
      });

  if (timeout_replace_enabled_) {
    PushEvent(
        "ERROR",
        now_ms,
        {
            {"event_name", "ERROR"},
            {"where", "order_manager.timeout"},
            {"upbit_error_message", "order_timeout_replace_required"},
            {"identifier", tracked.identifier},
            {"upbit_uuid", tracked.upbit_uuid},
            {"state", tracked.state},
            {"ts_ms", now_ms},
        });
    return;
  }

  const ManagedResult cancel_result = Cancel(tracked.upbit_uuid, tracked.identifier);
  if (!cancel_result.accepted) {
    PushEvent(
        "ERROR",
        now_ms,
        {
            {"event_name", "ERROR"},
            {"where", "order_manager.timeout_cancel"},
            {"upbit_error_message", cancel_result.reason},
            {"identifier", tracked.identifier},
            {"upbit_uuid", tracked.upbit_uuid},
            {"state", tracked.state},
            {"ts_ms", now_ms},
        });
  }
}

std::chrono::milliseconds OrderManager::CurrentPollingIntervalLocked() const {
  if (!private_ws_enabled_) {
    return std::chrono::milliseconds(std::max(poll_interval_rest_only_ms_, 200));
  }
  if (private_ws_connected_) {
    return std::chrono::milliseconds(std::max(poll_interval_ws_connected_ms_, 1000));
  }
  return std::chrono::milliseconds(std::max(poll_interval_ws_degraded_ms_, 1000));
}

void OrderManager::PollLoop() {
  while (true) {
    std::unordered_map<std::string, TrackedOrder> snapshot;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      std::chrono::milliseconds interval = CurrentPollingIntervalLocked();
      if (order_timeout_sec_ > 0 && !tracked_orders_.empty()) {
        interval = std::min(interval, std::chrono::milliseconds(1000));
      }
      cv_.wait_for(lock, interval, [this] { return stop_poller_; });
      if (stop_poller_) {
        break;
      }
      snapshot = tracked_orders_;
    }

    for (const auto& [tracking_key, tracked] : snapshot) {
      const std::int64_t now_ms = NowMs();
      if (order_timeout_sec_ > 0 &&
          IsOpenOrderState(tracked.state) &&
          tracked.last_state_ts_ms > 0 &&
          !tracked.timeout_emitted) {
        const std::int64_t timeout_ms = static_cast<std::int64_t>(order_timeout_sec_) * 1000;
        if (now_ms - tracked.last_state_ts_ms >= timeout_ms) {
          bool should_handle_timeout = false;
          TrackedOrder timeout_target = tracked;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = tracked_orders_.find(tracking_key);
            if (found != tracked_orders_.end() &&
                !found->second.timeout_emitted &&
                IsOpenOrderState(found->second.state) &&
                found->second.last_state_ts_ms > 0 &&
                now_ms - found->second.last_state_ts_ms >= timeout_ms) {
              found->second.timeout_emitted = true;
              if (!timeout_replace_enabled_) {
                found->second.cancel_requested = true;
              }
              timeout_target = found->second;
              should_handle_timeout = true;
            }
          }
          if (should_handle_timeout) {
            HandleOrderTimeout(timeout_target, now_ms);
            if (!timeout_replace_enabled_) {
              continue;
            }
          }
        }
      }

      const UpbitOrderResult order = rest_client_->GetOrder(tracked.upbit_uuid, tracked.identifier);
      if (!order.ok) {
        if (!order.retriable) {
          PushEvent(
              "ERROR",
              NowMs(),
              {
                  {"event_name", "ERROR"},
                  {"where", "order_manager.poll"},
                  {"http_status", order.http_status},
                  {"upbit_error_name", order.error_name},
                  {"upbit_error_message", order.reason},
                  {"breaker_state", order.breaker_state},
                  {"identifier", tracked.identifier},
                  {"upbit_uuid", tracked.upbit_uuid},
                  {"ts_ms", NowMs()},
              });
        }
        continue;
      }
      if (!order.found) {
        continue;
      }

      const std::string identifier = order.identifier.empty() ? tracked.identifier : order.identifier;
      const std::string upbit_uuid = order.upbit_uuid.empty() ? tracked.upbit_uuid : order.upbit_uuid;
      const std::string executed_volume_str = order.executed_volume_str.empty() ? "0" : order.executed_volume_str;
      const double executed_volume = ParseNumber(executed_volume_str);
      const std::string remaining_volume_str = order.remaining_volume_str.empty() ? "0" : order.remaining_volume_str;

      bool emit_state = false;
      bool emit_fill = false;
      double prev_executed_volume = 0.0;
      int replace_attempt_count = tracked.replace_attempt_count;
      std::string lifecycle_state;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = tracked_orders_.find(tracking_key);
        if (found == tracked_orders_.end()) {
          continue;
        }
        const bool state_changed = found->second.state != order.state;
        const bool executed_changed =
            found->second.last_executed_volume < 0.0 ||
            std::fabs(found->second.last_executed_volume - executed_volume) > 1e-12;
        emit_state = state_changed || executed_changed;
        prev_executed_volume = found->second.last_executed_volume < 0.0 ? 0.0 : found->second.last_executed_volume;
        emit_fill = executed_volume > prev_executed_volume + 1e-12;

        found->second.state = order.state;
        found->second.executed_volume_str = executed_volume_str;
        found->second.remaining_volume_str = remaining_volume_str;
        found->second.last_executed_volume = executed_volume;
        found->second.last_state_ts_ms = now_ms;
        found->second.cancel_requested = false;
        found->second.timeout_emitted = false;
        if (!upbit_uuid.empty()) {
          found->second.upbit_uuid = upbit_uuid;
        }
        if (emit_fill) {
          found->second.last_fill_ts_ms = now_ms;
        }
        replace_attempt_count = found->second.replace_attempt_count;
        lifecycle_state = ComputeLifecycleState(order.state, executed_volume_str, remaining_volume_str);

        if (!identifier.empty()) {
          auto order_it = order_by_identifier_.find(identifier);
          if (order_it != order_by_identifier_.end()) {
            order_it->second["state"] = order.state;
            order_it->second["executed_volume_str"] = executed_volume_str;
            order_it->second["remaining_volume_str"] = remaining_volume_str;
            order_it->second["avg_price_str"] =
                order.avg_price_str.empty() ? nlohmann::json(nullptr) : nlohmann::json(order.avg_price_str);
            order_it->second["lifecycle_state"] = lifecycle_state;
            order_it->second["replace_attempt_count"] = replace_attempt_count;
          }
        }

        if (IsClosedOrderState(order.state)) {
          tracked_orders_.erase(found);
        }
      }

      if (emit_state) {
        nlohmann::json payload = {
            {"event_name", "ORDER_STATE"},
            {"uuid", upbit_uuid},
            {"upbit_uuid", upbit_uuid},
            {"identifier", identifier},
            {"market", order.market},
            {"state", order.state},
            {"executed_volume_str", executed_volume_str},
            {"remaining_volume_str", remaining_volume_str},
            {"avg_price_str", order.avg_price_str.empty() ? nlohmann::json(nullptr) : nlohmann::json(order.avg_price_str)},
            {"lifecycle_state", lifecycle_state},
            {"replace_attempt_count", replace_attempt_count},
            {"updated_ts_ms", now_ms},
            {"mode", rest_client_->ModeName()},
            {"price", order.price_str},
            {"volume", order.volume_str},
            {"executed_volume", executed_volume_str},
          };
        if (order.remaining_req_sec >= 0) {
          payload["remaining_req"] = {
              {"group", order.remaining_req_group},
              {"sec", order.remaining_req_sec},
          };
        }
        PushEvent("ORDER_UPDATE", NowMs(), payload);
      }

      if (emit_fill) {
        const double fill_volume = std::max(executed_volume - prev_executed_volume, 0.0);
        const std::string fill_volume_str = upbit::FormatVolumeString(fill_volume, 16);
        nlohmann::json payload = {
            {"event_name", "FILL"},
            {"uuid", upbit_uuid},
            {"upbit_uuid", upbit_uuid},
            {"identifier", identifier},
            {"market", order.market},
            {"price_str", order.avg_price_str.empty() ? order.price_str : order.avg_price_str},
            {"volume_str", fill_volume_str},
            {"fee_str", nlohmann::json(nullptr)},
            {"ts_ms", now_ms},
            {"price", order.avg_price_str.empty() ? order.price_str : order.avg_price_str},
            {"volume", fill_volume_str},
            {"executed_volume", executed_volume_str},
        };
        PushEvent("FILL", now_ms, payload);
      }
    }
  }
}

void OrderManager::TrackOrder(const ManagedIntent& intent, const ManagedResult& result) {
  if (rest_client_ == nullptr || rest_client_->IsOrderTestMode()) {
    return;
  }
  const std::string key = TrackingKey(result.upbit_uuid, result.identifier);
  if (key.empty()) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  TrackedOrder& tracked = tracked_orders_[key];
  tracked.upbit_uuid = result.upbit_uuid;
  tracked.identifier = result.identifier;
  if (!intent.market.empty()) {
    tracked.market = intent.market;
  }
  if (!intent.side.empty()) {
    tracked.side = intent.side;
  }
  if (!intent.ord_type.empty()) {
    tracked.ord_type = intent.ord_type;
  }
  if (intent.price > 0.0) {
    tracked.price_str = upbit::FormatPriceString(intent.price, 0.0, 16);
  }
  if (intent.volume > 0.0) {
    tracked.volume_str = upbit::FormatVolumeString(intent.volume, 16);
  }
  tracked.state = "wait";
  tracked.executed_volume_str = "0";
  tracked.remaining_volume_str = tracked.volume_str;
  tracked.last_executed_volume = 0.0;
  tracked.timeout_emitted = false;
  tracked.cancel_requested = false;
  tracked.created_ts_ms = NowMs();
  tracked.last_state_ts_ms = tracked.created_ts_ms;
  tracked.last_fill_ts_ms = 0;
}

std::string OrderManager::TrackingKey(const std::string& upbit_uuid, const std::string& identifier) {
  if (!identifier.empty()) {
    return "id:" + identifier;
  }
  if (!upbit_uuid.empty()) {
    return "uuid:" + upbit_uuid;
  }
  return "";
}

std::int64_t OrderManager::NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

}  // namespace autobot::executor
