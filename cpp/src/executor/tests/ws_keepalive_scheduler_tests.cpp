#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <string>
#include <thread>

#include "upbit/ws_keepalive_scheduler.h"

namespace {

std::int64_t NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

bool WaitUntil(const std::function<bool()>& predicate, int timeout_ms) {
  const auto started = std::chrono::steady_clock::now();
  while (!predicate()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    if (elapsed >= timeout_ms) {
      return false;
    }
  }
  return true;
}

void TestIdlePingTriggerAndNoResubscribe() {
  using autobot::executor::upbit::WsKeepaliveMode;
  using autobot::executor::upbit::WsKeepaliveScheduler;
  using autobot::executor::upbit::WsKeepaliveSchedulerOptions;

  std::atomic<bool> stop(false);
  std::atomic<bool> connected(true);
  std::atomic<std::int64_t> last_rx(NowMs() - 5000);
  std::atomic<std::int64_t> last_tx(NowMs() - 5000);
  std::atomic<std::int64_t> last_ping(0);
  std::atomic<std::int64_t> last_pong(0);
  std::atomic<int> ping_sent_count(0);
  std::atomic<int> subscribe_call_count(0);

  WsKeepaliveSchedulerOptions options;
  options.mode = WsKeepaliveMode::kMessage;
  options.ping_on_connect = false;
  options.ping_interval_sec = 1;
  options.pong_grace_sec = 2;
  options.force_reconnect_on_stale = false;
  options.stale_rx_threshold_sec = 120;
  options.tick_ms = 100;

  WsKeepaliveScheduler::Callbacks callbacks;
  callbacks.is_connected = [&connected]() { return connected.load(); };
  callbacks.last_rx_ts_ms = [&last_rx]() { return last_rx.load(); };
  callbacks.last_tx_ts_ms = [&last_tx]() { return last_tx.load(); };
  callbacks.last_ping_ts_ms = [&last_ping]() { return last_ping.load(); };
  callbacks.last_pong_ts_ms = [&last_pong]() { return last_pong.load(); };
  callbacks.send_ping = [&](WsKeepaliveMode mode, std::string* error) {
    (void)error;
    assert(mode == WsKeepaliveMode::kMessage);
    const std::int64_t now_ms = NowMs();
    last_tx.store(now_ms);
    last_ping.store(now_ms);
    ping_sent_count.fetch_add(1);
    return true;
  };
  callbacks.on_stale = [&](const std::string&) { subscribe_call_count.fetch_add(1000); };
  callbacks.on_error = [&](const std::string&) { subscribe_call_count.fetch_add(1000); };

  WsKeepaliveScheduler scheduler(options, callbacks);
  scheduler.Start(&stop);
  const bool ping_fired = WaitUntil([&ping_sent_count]() { return ping_sent_count.load() >= 1; }, 2000);
  stop.store(true);
  scheduler.Stop();

  assert(ping_fired);
  assert(subscribe_call_count.load() == 0);
}

void TestPingOnConnectStaleAckTimeout() {
  using autobot::executor::upbit::WsKeepaliveMode;
  using autobot::executor::upbit::WsKeepaliveScheduler;
  using autobot::executor::upbit::WsKeepaliveSchedulerOptions;

  std::atomic<bool> stop(false);
  std::atomic<bool> connected(true);
  std::atomic<std::int64_t> last_rx(0);
  std::atomic<std::int64_t> last_tx(0);
  std::atomic<std::int64_t> last_ping(0);
  std::atomic<std::int64_t> last_pong(0);
  std::atomic<int> stale_count(0);
  std::string stale_reason;

  WsKeepaliveSchedulerOptions options;
  options.mode = WsKeepaliveMode::kMessage;
  options.ping_on_connect = true;
  options.ping_interval_sec = 60;
  options.pong_grace_sec = 1;
  options.force_reconnect_on_stale = true;
  options.stale_rx_threshold_sec = 120;
  options.tick_ms = 100;

  WsKeepaliveScheduler::Callbacks callbacks;
  callbacks.is_connected = [&connected]() { return connected.load(); };
  callbacks.last_rx_ts_ms = [&last_rx]() { return last_rx.load(); };
  callbacks.last_tx_ts_ms = [&last_tx]() { return last_tx.load(); };
  callbacks.last_ping_ts_ms = [&last_ping]() { return last_ping.load(); };
  callbacks.last_pong_ts_ms = [&last_pong]() { return last_pong.load(); };
  callbacks.send_ping = [&](WsKeepaliveMode mode, std::string* error) {
    (void)error;
    assert(mode == WsKeepaliveMode::kMessage);
    const std::int64_t now_ms = NowMs();
    last_tx.store(now_ms);
    last_ping.store(now_ms);
    return true;
  };
  callbacks.on_stale = [&](const std::string& reason) {
    stale_reason = reason;
    stale_count.fetch_add(1);
  };

  WsKeepaliveScheduler scheduler(options, callbacks);
  scheduler.Start(&stop);
  const bool stale_fired = WaitUntil([&stale_count]() { return stale_count.load() >= 1; }, 3000);
  stop.store(true);
  scheduler.Stop();

  assert(stale_fired);
  assert(stale_reason.find("ack_timeout") != std::string::npos);
}

}  // namespace

int main() {
  TestIdlePingTriggerAndNoResubscribe();
  TestPingOnConnectStaleAckTimeout();
  return 0;
}
