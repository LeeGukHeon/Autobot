#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>

namespace autobot::executor::upbit {

enum class WsKeepaliveMode {
  kOff = 0,
  kMessage = 1,
  kFrame = 2,
};

struct WsKeepaliveSchedulerOptions {
  WsKeepaliveMode mode = WsKeepaliveMode::kMessage;
  bool ping_on_connect = true;
  int ping_interval_sec = 60;
  int pong_grace_sec = 20;
  bool force_reconnect_on_stale = true;
  int stale_rx_threshold_sec = 110;
  int tick_ms = 1000;
};

class WsKeepaliveScheduler {
 public:
  struct Callbacks {
    std::function<bool()> is_connected;
    std::function<std::int64_t()> last_rx_ts_ms;
    std::function<std::int64_t()> last_tx_ts_ms;
    std::function<std::int64_t()> last_ping_ts_ms;
    std::function<std::int64_t()> last_pong_ts_ms;
    std::function<bool(WsKeepaliveMode mode, std::string* error)> send_ping;
    std::function<void(const std::string& reason)> on_stale;
    std::function<void(const std::string& reason)> on_error;
  };

  WsKeepaliveScheduler(WsKeepaliveSchedulerOptions options, Callbacks callbacks);
  ~WsKeepaliveScheduler();

  void Start(const std::atomic<bool>* stop_flag);
  void Stop();

 private:
  static std::int64_t NowMs();
  void Loop();
  void SleepTick() const;
  void TrySendPing(bool* waiting_ack, std::int64_t* expected_ack_ping_ts, bool* stale_emitted);
  void MaybeEmitStale(const std::string& reason, bool* stale_emitted);

  WsKeepaliveSchedulerOptions options_;
  Callbacks callbacks_;
  const std::atomic<bool>* external_stop_flag_ = nullptr;
  std::atomic<bool> stop_local_{false};
  std::thread thread_;
};

}  // namespace autobot::executor::upbit
