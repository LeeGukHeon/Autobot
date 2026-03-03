#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "upbit/ws_keepalive_scheduler.h"
#include "upbit/ws_private_parsers.h"

namespace autobot::executor::upbit {

struct WsPrivateClientOptions {
  std::string url = "wss://api.upbit.com/websocket/v1/private";
  bool subscribe_my_order = true;
  bool subscribe_my_asset = true;
  std::vector<std::string> my_order_codes;
  std::string subscribe_format = "DEFAULT";
  bool format_fallback_to_default_once = true;
  std::string ticket_prefix = "autobot-executor-private";

  int connect_rps = 5;
  int message_rps = 5;
  int message_rpm = 100;

  int connect_timeout_ms = 3000;
  int read_timeout_ms = 1000;
  int write_timeout_ms = 3000;
  std::string keepalive_mode = "message";
  bool ping_on_connect = true;
  int ping_interval_sec = 60;
  int pong_grace_sec = 20;
  int stale_rx_threshold_sec = 110;
  bool force_reconnect_on_stale = true;
  bool up_status_log = false;
  int idle_timeout_sec = 125;

  bool reconnect_enabled = true;
  int reconnect_base_delay_ms = 1000;
  int reconnect_max_delay_ms = 15000;
  int reconnect_jitter_ms = 300;
};

struct WsPrivateClientStats {
  int reconnect_count = 0;
  std::int64_t received_events = 0;
  std::int64_t last_event_ts_ms = 0;
  std::int64_t last_connect_ts_ms = 0;
  std::int64_t last_disconnect_ts_ms = 0;
  std::int64_t last_rx_ts_ms = 0;
  std::int64_t last_tx_ts_ms = 0;
  std::int64_t last_ping_ts_ms = 0;
  std::int64_t last_pong_ts_ms = 0;
  std::int64_t ping_sent_count = 0;
  std::int64_t pong_rx_count = 0;
  std::int64_t stale_disconnect_count = 0;
  std::string keepalive_mode = "message";
  int keepalive_ping_interval_sec = 60;
  int keepalive_pong_grace_sec = 20;
  bool keepalive_ping_on_connect = true;
  bool keepalive_force_reconnect_on_stale = true;
  bool keepalive_up_status_log = false;
  std::string last_disconnect_reason;
};

class UpbitPrivateWsClient {
 public:
  struct Callbacks {
    std::function<void()> on_connected;
    std::function<void(const std::string& reason)> on_disconnected;
    std::function<void(const std::string& reason)> on_error;
    std::function<void(const WsHealthUpEvent& event)> on_health_up;
    std::function<void(const WsMyOrderEvent& event)> on_my_order;
    std::function<void(const WsMyAssetEvent& event)> on_my_asset;
  };

  explicit UpbitPrivateWsClient(WsPrivateClientOptions options);
  virtual ~UpbitPrivateWsClient() = default;

  virtual void Run(
      const std::function<std::string()>& authorization_header_provider,
      const std::atomic<bool>* stop_flag,
      const Callbacks& callbacks);
  virtual WsPrivateClientStats Stats() const;

 private:
  void EmitError(const Callbacks& callbacks, const std::string& reason) const;
  void MarkConnected();
  void MarkDisconnected(const std::string& reason);
  void MarkReceivedEvent(std::int64_t event_ts_ms);
  void MarkRx();
  void MarkTx();
  void MarkPingSent();
  void MarkPongReceived();
  void MarkStaleDisconnect();
  int ReconnectDelayMs(int attempt_index) const;

  WsPrivateClientOptions options_;
  mutable std::mutex stats_mutex_;
  WsPrivateClientStats stats_;
};

}  // namespace autobot::executor::upbit
