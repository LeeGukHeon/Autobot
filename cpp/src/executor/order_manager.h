#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <nlohmann/json.hpp>

#include "upbit/ws_private_client.h"
#include "upbit_rest.h"

namespace autobot::executor {

struct ManagedIntent {
  std::string intent_id;
  std::string identifier;
  std::string market;
  std::string side;
  std::string ord_type;
  double price = 0.0;
  double volume = 0.0;
  std::string tif;
  std::int64_t ts_ms = 0;
  std::string meta_json;
};

struct ManagedResult {
  bool accepted = false;
  std::string reason;
  std::string upbit_uuid;
  std::string identifier;
  std::string intent_id;
};

struct ManagedReplaceRequest {
  std::string intent_id;
  std::string prev_order_uuid;
  std::string prev_order_identifier;
  std::string new_identifier;
  std::string new_price_str;
  std::string new_volume_str;
  std::string new_time_in_force;
};

struct ManagedReplaceResult {
  bool accepted = false;
  std::string reason;
  std::string cancelled_order_uuid;
  std::string new_order_uuid;
  std::string new_identifier;
};

struct ManagedEvent {
  std::string event_type;
  std::int64_t ts_ms = 0;
  nlohmann::json payload;
};

class OrderManager {
 public:
  explicit OrderManager(
      UpbitRestClient* rest_client,
      std::function<std::unique_ptr<upbit::UpbitPrivateWsClient>(
          const upbit::WsPrivateClientOptions&)> private_ws_factory = {});
  ~OrderManager();

  ManagedResult SubmitIntent(const ManagedIntent& intent);
  ManagedResult Cancel(const std::string& upbit_uuid, const std::string& identifier);
  ManagedReplaceResult ReplaceOrder(const ManagedReplaceRequest& request);
  bool PopEvent(ManagedEvent* out, std::chrono::milliseconds timeout);
  nlohmann::json Snapshot() const;

 private:
  struct TrackedOrder {
    std::string upbit_uuid;
    std::string identifier;
    std::string market;
    std::string side;
    std::string ord_type;
    std::string price_str;
    std::string volume_str;
    std::string state;
    std::string executed_volume_str = "0";
    std::string remaining_volume_str = "0";
    double last_executed_volume = -1.0;
    int replace_attempt_count = 0;
    bool timeout_emitted = false;
    bool cancel_requested = false;
    std::int64_t created_ts_ms = 0;
    std::int64_t last_state_ts_ms = 0;
    std::int64_t last_fill_ts_ms = 0;
  };

  void PushEvent(const std::string& event_type, std::int64_t ts_ms, nlohmann::json payload);
  void StartPollingIfNeeded();
  void StartPrivateWsIfNeeded();
  void StopPolling();
  void StopPrivateWs();
  void PollLoop();
  void PrivateWsLoop();
  void BootstrapAssetSnapshot();
  void HandleWsOrderEvent(const upbit::WsMyOrderEvent& event);
  void HandleWsAssetEvent(const upbit::WsMyAssetEvent& event);
  std::chrono::milliseconds CurrentPollingIntervalLocked() const;
  std::string ComputeLifecycleState(
      const std::string& state,
      const std::string& executed_volume_str,
      const std::string& remaining_volume_str) const;
  void HandleOrderTimeout(const TrackedOrder& tracked, std::int64_t now_ms);
  void TrackOrder(const ManagedIntent& intent, const ManagedResult& result);
  static std::string TrackingKey(const std::string& upbit_uuid, const std::string& identifier);
  static std::int64_t NowMs();

  UpbitRestClient* rest_client_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::unordered_map<std::string, ManagedResult> by_identifier_;
  std::unordered_map<std::string, nlohmann::json> order_by_identifier_;
  std::unordered_map<std::string, TrackedOrder> tracked_orders_;
  std::deque<ManagedEvent> events_;
  bool stop_poller_ = false;
  std::thread poller_thread_;
  bool private_ws_enabled_ = false;
  bool private_ws_connected_ = false;
  std::atomic<bool> stop_private_ws_{false};
  std::thread private_ws_thread_;
  std::unique_ptr<upbit::UpbitPrivateWsClient> private_ws_client_;
  std::function<std::unique_ptr<upbit::UpbitPrivateWsClient>(
      const upbit::WsPrivateClientOptions&)>
      private_ws_factory_;
  bool private_ws_up_status_log_ = false;
  int poll_interval_rest_only_ms_ = 1500;
  int poll_interval_ws_connected_ms_ = 180000;
  int poll_interval_ws_degraded_ms_ = 60000;
  int order_timeout_sec_ = 0;
  bool timeout_replace_enabled_ = false;
};

}  // namespace autobot::executor
