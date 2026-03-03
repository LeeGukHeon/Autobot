#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>
#include <string>
#include <unordered_map>

#include <nlohmann/json.hpp>

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

struct ManagedEvent {
  std::string event_type;
  std::int64_t ts_ms = 0;
  nlohmann::json payload;
};

class OrderManager {
 public:
  explicit OrderManager(UpbitRestClient* rest_client);
  ~OrderManager();

  ManagedResult SubmitIntent(const ManagedIntent& intent);
  ManagedResult Cancel(const std::string& upbit_uuid, const std::string& identifier);
  bool PopEvent(ManagedEvent* out, std::chrono::milliseconds timeout);
  nlohmann::json Snapshot() const;

 private:
  struct TrackedOrder {
    std::string upbit_uuid;
    std::string identifier;
    std::string market;
    std::string side;
    std::string ord_type;
    double price = 0.0;
    double volume = 0.0;
    std::string last_state;
    double last_executed_volume = -1.0;
  };

  void PushEvent(const std::string& event_type, std::int64_t ts_ms, nlohmann::json payload);
  void StartPollingIfNeeded();
  void StopPolling();
  void PollLoop();
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
};

}  // namespace autobot::executor
