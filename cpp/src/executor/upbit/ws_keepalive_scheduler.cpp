#include "upbit/ws_keepalive_scheduler.h"

#include <algorithm>
#include <chrono>
#include <thread>
#include <utility>

namespace autobot::executor::upbit {

namespace {

bool IsStopped(const std::atomic<bool>* external_stop_flag, const std::atomic<bool>* local_stop_flag) {
  if (local_stop_flag != nullptr && local_stop_flag->load()) {
    return true;
  }
  if (external_stop_flag != nullptr && external_stop_flag->load()) {
    return true;
  }
  return false;
}

}  // namespace

WsKeepaliveScheduler::WsKeepaliveScheduler(
    WsKeepaliveSchedulerOptions options,
    WsKeepaliveScheduler::Callbacks callbacks)
    : options_(std::move(options)), callbacks_(std::move(callbacks)) {
  options_.ping_interval_sec = std::max(options_.ping_interval_sec, 1);
  options_.pong_grace_sec = std::max(options_.pong_grace_sec, 1);
  options_.stale_rx_threshold_sec = std::max(options_.stale_rx_threshold_sec, 1);
  options_.tick_ms = std::max(options_.tick_ms, 100);
}

WsKeepaliveScheduler::~WsKeepaliveScheduler() {
  Stop();
}

void WsKeepaliveScheduler::Start(const std::atomic<bool>* stop_flag) {
  Stop();
  external_stop_flag_ = stop_flag;
  stop_local_.store(false);
  thread_ = std::thread(&WsKeepaliveScheduler::Loop, this);
}

void WsKeepaliveScheduler::Stop() {
  stop_local_.store(true);
  if (thread_.joinable()) {
    thread_.join();
  }
}

std::int64_t WsKeepaliveScheduler::NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

void WsKeepaliveScheduler::Loop() {
  bool ping_on_connect_sent = false;
  bool waiting_ack = false;
  bool stale_emitted = false;
  std::int64_t expected_ack_ping_ts = 0;

  while (!IsStopped(external_stop_flag_, &stop_local_)) {
    const bool connected = callbacks_.is_connected ? callbacks_.is_connected() : false;
    if (!connected) {
      ping_on_connect_sent = false;
      waiting_ack = false;
      stale_emitted = false;
      expected_ack_ping_ts = 0;
      SleepTick();
      continue;
    }

    const std::int64_t now_ms = NowMs();
    const std::int64_t last_rx_ts_ms = callbacks_.last_rx_ts_ms ? callbacks_.last_rx_ts_ms() : 0;
    const std::int64_t last_tx_ts_ms = callbacks_.last_tx_ts_ms ? callbacks_.last_tx_ts_ms() : 0;
    const std::int64_t last_ping_ts_ms = callbacks_.last_ping_ts_ms ? callbacks_.last_ping_ts_ms() : 0;
    const std::int64_t last_pong_ts_ms = callbacks_.last_pong_ts_ms ? callbacks_.last_pong_ts_ms() : 0;

    if (options_.mode != WsKeepaliveMode::kOff) {
      if (options_.ping_on_connect && !ping_on_connect_sent) {
        TrySendPing(&waiting_ack, &expected_ack_ping_ts, &stale_emitted);
        ping_on_connect_sent = true;
      } else {
        const std::int64_t baseline = std::max(last_rx_ts_ms, last_tx_ts_ms);
        if (baseline > 0) {
          const std::int64_t idle_ms = now_ms - baseline;
          if (idle_ms >= static_cast<std::int64_t>(options_.ping_interval_sec) * 1000) {
            TrySendPing(&waiting_ack, &expected_ack_ping_ts, &stale_emitted);
          }
        }
      }

      if (waiting_ack && expected_ack_ping_ts > 0) {
        const std::int64_t ack_ts_ms = std::max(last_pong_ts_ms, last_rx_ts_ms);
        if (ack_ts_ms >= expected_ack_ping_ts) {
          waiting_ack = false;
        } else if (options_.force_reconnect_on_stale) {
          const std::int64_t grace_ms = static_cast<std::int64_t>(options_.pong_grace_sec) * 1000;
          if (now_ms - expected_ack_ping_ts >= grace_ms) {
            MaybeEmitStale("ws_keepalive_ack_timeout", &stale_emitted);
          }
        }
      }
    }

    if (options_.mode != WsKeepaliveMode::kOff &&
        options_.force_reconnect_on_stale &&
        last_rx_ts_ms > 0) {
      const std::int64_t stale_rx_ms = static_cast<std::int64_t>(options_.stale_rx_threshold_sec) * 1000;
      if (now_ms - last_rx_ts_ms >= stale_rx_ms) {
        MaybeEmitStale("ws_keepalive_rx_stale", &stale_emitted);
      }
    }

    if (last_ping_ts_ms > 0 && last_pong_ts_ms >= last_ping_ts_ms) {
      waiting_ack = false;
      stale_emitted = false;
      expected_ack_ping_ts = 0;
    }

    SleepTick();
  }
}

void WsKeepaliveScheduler::SleepTick() const {
  int remain_ms = std::max(options_.tick_ms, 100);
  while (remain_ms > 0 && !IsStopped(external_stop_flag_, &stop_local_)) {
    const int slice_ms = std::min(remain_ms, 100);
    std::this_thread::sleep_for(std::chrono::milliseconds(slice_ms));
    remain_ms -= slice_ms;
  }
}

void WsKeepaliveScheduler::TrySendPing(
    bool* waiting_ack,
    std::int64_t* expected_ack_ping_ts,
    bool* stale_emitted) {
  if (waiting_ack == nullptr || expected_ack_ping_ts == nullptr || stale_emitted == nullptr) {
    return;
  }
  if (!callbacks_.send_ping) {
    return;
  }

  std::string send_error;
  if (!callbacks_.send_ping(options_.mode, &send_error)) {
    if (callbacks_.on_error) {
      callbacks_.on_error(
          send_error.empty() ? "ws_keepalive_ping_send_failed" : "ws_keepalive_ping_send_failed:" + send_error);
    }
    if (options_.force_reconnect_on_stale) {
      MaybeEmitStale(
          send_error.empty() ? "ws_keepalive_ping_send_failed" : "ws_keepalive_ping_send_failed:" + send_error,
          stale_emitted);
    }
    return;
  }

  *waiting_ack = true;
  *stale_emitted = false;
  if (callbacks_.last_ping_ts_ms) {
    const std::int64_t last_ping_ts_ms = callbacks_.last_ping_ts_ms();
    *expected_ack_ping_ts = last_ping_ts_ms > 0 ? last_ping_ts_ms : NowMs();
  } else {
    *expected_ack_ping_ts = NowMs();
  }
}

void WsKeepaliveScheduler::MaybeEmitStale(const std::string& reason, bool* stale_emitted) {
  if (stale_emitted == nullptr) {
    return;
  }
  if (*stale_emitted) {
    return;
  }
  *stale_emitted = true;
  if (callbacks_.on_stale) {
    callbacks_.on_stale(reason.empty() ? "ws_keepalive_stale" : reason);
  }
}

}  // namespace autobot::executor::upbit
