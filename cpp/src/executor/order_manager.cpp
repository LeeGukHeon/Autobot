#include "order_manager.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <utility>

namespace autobot::executor {

namespace {

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

bool IsClosedOrderState(const std::string& state) {
  const std::string lowered = ToLower(state);
  return lowered == "done" || lowered == "cancel" || lowered == "cancelled" || lowered == "reject";
}

}  // namespace

OrderManager::OrderManager(UpbitRestClient* rest_client) : rest_client_(rest_client) {
  StartPollingIfNeeded();
}

OrderManager::~OrderManager() {
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

  const std::string order_state = submit.state.empty() ? (submit.accepted ? "wait" : "reject") : submit.state;
  nlohmann::json payload = {
      {"uuid", submit.upbit_uuid},
      {"upbit_uuid", submit.upbit_uuid},
      {"identifier", result.identifier},
      {"intent_id", intent.intent_id},
      {"market", intent.market},
      {"side", intent.side},
      {"ord_type", intent.ord_type},
      {"state", order_state},
      {"price", intent.price},
      {"volume", intent.volume},
      {"executed_volume", 0.0},
      {"reason", submit.reason},
      {"event_name", submit.accepted ? "ORDER_ACCEPTED" : "ORDER_REJECTED"},
  };
  if (submit.remaining_req_sec >= 0) {
    payload["remaining_req"] = {
        {"group", submit.remaining_req_group},
        {"sec", submit.remaining_req_sec},
    };
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    by_identifier_[result.identifier] = result;
    order_by_identifier_[result.identifier] = payload;
  }
  PushEvent("ORDER_UPDATE", NowMs(), payload);
  if (submit.accepted) {
    TrackOrder(intent, result);
  } else {
    PushEvent(
        "ERROR",
        NowMs(),
        {{"event_name", "ORDER_REJECTED"},
         {"identifier", result.identifier},
         {"upbit_uuid", result.upbit_uuid},
         {"reason", submit.reason}});
  }
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

  nlohmann::json payload = {
      {"uuid", cancel.upbit_uuid},
      {"upbit_uuid", cancel.upbit_uuid},
      {"identifier", resolved_identifier},
      {"state", cancel_state},
      {"reason", cancel.reason},
      {"event_name", "CANCEL_RESULT"},
  };
  if (cancel.remaining_req_sec >= 0) {
    payload["remaining_req"] = {
        {"group", cancel.remaining_req_group},
        {"sec", cancel.remaining_req_sec},
    };
  }

  ManagedIntent track_intent;
  track_intent.identifier = resolved_identifier;
  track_intent.market = "";
  track_intent.side = "";
  track_intent.ord_type = "";
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
        order_it->second["state"] = payload["state"];
        if (order_it->second.contains("market")) {
          track_intent.market = order_it->second.value("market", "");
        }
        if (order_it->second.contains("side")) {
          track_intent.side = order_it->second.value("side", "");
        }
        if (order_it->second.contains("ord_type")) {
          track_intent.ord_type = order_it->second.value("ord_type", "");
        }
      }
    }
  }

  PushEvent("ORDER_UPDATE", NowMs(), payload);
  if (cancel.accepted) {
    TrackOrder(track_intent, result);
  } else {
    PushEvent(
        "ERROR",
        NowMs(),
        {{"event_name", "CANCEL_REJECTED"},
         {"identifier", result.identifier},
         {"upbit_uuid", result.upbit_uuid},
         {"reason", result.reason}});
  }
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
  return {{"orders", orders}, {"intents", intents}};
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
  std::lock_guard<std::mutex> lock(mutex_);
  if (poller_thread_.joinable()) {
    return;
  }
  stop_poller_ = false;
  poller_thread_ = std::thread(&OrderManager::PollLoop, this);
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

void OrderManager::PollLoop() {
  while (true) {
    std::unordered_map<std::string, TrackedOrder> snapshot;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(1500), [this] { return stop_poller_; });
      if (stop_poller_) {
        break;
      }
      snapshot = tracked_orders_;
    }

    for (const auto& [tracking_key, tracked] : snapshot) {
      const UpbitOrderResult order = rest_client_->GetOrder(tracked.upbit_uuid, tracked.identifier);
      if (!order.ok) {
        if (!order.retriable) {
          PushEvent(
              "ERROR",
              NowMs(),
              {{"event_name", "ORDER_STATE_POLL_FAILED"},
               {"identifier", tracked.identifier},
               {"upbit_uuid", tracked.upbit_uuid},
               {"reason", order.reason}});
        }
        continue;
      }
      if (!order.found) {
        continue;
      }

      bool emit_state = false;
      bool emit_fill = false;
      double prev_executed_volume = 0.0;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = tracked_orders_.find(tracking_key);
        if (found == tracked_orders_.end()) {
          continue;
        }
        const bool state_changed = found->second.last_state != order.state;
        const bool executed_changed =
            found->second.last_executed_volume < 0.0 ||
            std::fabs(found->second.last_executed_volume - order.executed_volume) > 1e-12;
        emit_state = state_changed || executed_changed;
        prev_executed_volume = found->second.last_executed_volume < 0.0 ? 0.0 : found->second.last_executed_volume;
        emit_fill = order.executed_volume > prev_executed_volume + 1e-12;

        found->second.last_state = order.state;
        found->second.last_executed_volume = order.executed_volume;
        if (!order.upbit_uuid.empty()) {
          found->second.upbit_uuid = order.upbit_uuid;
        }

        if (!order.identifier.empty()) {
          auto order_it = order_by_identifier_.find(order.identifier);
          if (order_it != order_by_identifier_.end()) {
            order_it->second["state"] = order.state;
            order_it->second["executed_volume"] = order.executed_volume;
          }
        }

        if (IsClosedOrderState(order.state)) {
          tracked_orders_.erase(found);
        }
      }

      if (emit_state) {
        nlohmann::json payload = {
            {"event_name", "ORDER_STATE"},
            {"uuid", order.upbit_uuid},
            {"upbit_uuid", order.upbit_uuid},
            {"identifier", order.identifier},
            {"market", order.market},
            {"side", order.side},
            {"ord_type", order.ord_type},
            {"state", order.state},
            {"price", order.price},
            {"volume", order.volume},
            {"executed_volume", order.executed_volume},
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
        nlohmann::json payload = {
            {"event_name", "FILL"},
            {"uuid", order.upbit_uuid},
            {"upbit_uuid", order.upbit_uuid},
            {"identifier", order.identifier},
            {"market", order.market},
            {"side", order.side},
            {"ord_type", order.ord_type},
            {"state", order.state},
            {"price", order.price},
            {"volume", order.volume},
            {"executed_volume", order.executed_volume},
            {"fill_volume", std::max(order.executed_volume - prev_executed_volume, 0.0)},
        };
        PushEvent("FILL", NowMs(), payload);
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
    tracked.price = intent.price;
  }
  if (intent.volume > 0.0) {
    tracked.volume = intent.volume;
  }
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
