#include <cassert>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "order_manager.h"
#include "state/executor_state_store.h"
#include "tests/fault_injection_transport.h"
#include "upbit/http_client.h"
#include "upbit_rest.h"

namespace {

void SetEnv(const char* key, const char* value) {
#ifdef _WIN32
  _putenv_s(key, value);
#else
  setenv(key, value, 1);
#endif
}

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

std::filesystem::path MakeStatePath(const std::string& name) {
  namespace fs = std::filesystem;
  const fs::path dir = fs::path("data") / "state";
  std::error_code ec;
  fs::create_directories(dir, ec);
  return dir / ("executor_fault_" + name + ".json");
}

void ConfigureLiveTestEnv(const std::filesystem::path& state_path) {
  SetEnv("AUTOBOT_LIVE_ENABLE", "YES");
  SetEnv("UPBIT_ACCESS_KEY", "DUMMY_ACCESS");
  SetEnv("UPBIT_SECRET_KEY", "DUMMY_SECRET");
  SetEnv("AUTOBOT_EXECUTOR_STATE_PATH", state_path.string().c_str());
  SetEnv("AUTOBOT_EXECUTOR_POLL_INTERVAL_REST_ONLY_MS", "30000");
  SetEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLED", "false");
  SetEnv("AUTOBOT_UPBIT_BAN_COOLDOWN_SEC", "1");
}

autobot::executor::upbit::HttpClientOptions BuildHttpOptions() {
  autobot::executor::upbit::HttpClientOptions options;
  options.base_url = "https://api.upbit.com";
  options.max_attempts = 1;
  options.rate_limit_enabled = true;
  options.ban_cooldown_sec = 1;
  options.access_key = "DUMMY_ACCESS";
  options.secret_key = "DUMMY_SECRET";
  options.group_rates = {
      {"default", 30.0},
      {"order", 8.0},
      {"order-test", 8.0},
      {"order-cancel-all", 0.5},
  };
  return options;
}

bool DrainUntilEvent(
    autobot::executor::OrderManager* manager,
    const std::string& event_type,
    const std::string& event_name,
    int timeout_ms) {
  if (manager == nullptr) {
    return false;
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  autobot::executor::ManagedEvent event;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!manager->PopEvent(&event, std::chrono::milliseconds(50))) {
      continue;
    }
    if (event.event_type != event_type) {
      continue;
    }
    if (!event_name.empty()) {
      const std::string payload_name = event.payload.value("event_name", "");
      if (payload_name != event_name) {
        continue;
      }
    }
    return true;
  }
  return false;
}

void CleanupStateFile(const std::filesystem::path& state_path) {
  namespace fs = std::filesystem;
  std::error_code ec;
  fs::remove(state_path, ec);
  ec.clear();
  fs::remove(fs::path(state_path.string() + ".bak"), ec);
  ec.clear();
  fs::remove(fs::path(state_path.string() + ".tmp"), ec);
  ec.clear();
}

void TestD1PostTimeoutRecoverByGetIdentifier() {
  using autobot::executor::ManagedIntent;
  using autobot::executor::OrderManager;
  using autobot::executor::UpbitRestClient;
  using autobot::executor::tests::FaultInjectionAction;
  using autobot::executor::tests::FaultInjectionRule;
  using autobot::executor::tests::FaultInjectionTransport;

  const std::filesystem::path state_path = MakeStatePath("d1");
  CleanupStateFile(state_path);
  ConfigureLiveTestEnv(state_path);

  auto transport = std::make_unique<FaultInjectionTransport>();
  FaultInjectionTransport* transport_ptr = transport.get();
  FaultInjectionRule post_timeout;
  post_timeout.method = "POST";
  post_timeout.endpoint = "/v1/orders";
  post_timeout.nth_call = 1;
  post_timeout.probability = 1.0;
  post_timeout.action.kind = FaultInjectionAction::Kind::kNetworkError;
  post_timeout.action.network_error = "injected_timeout";
  transport->AddRule(post_timeout);

  FaultInjectionRule get_recover;
  get_recover.method = "GET";
  get_recover.endpoint = "/v1/order";
  get_recover.nth_call = 1;
  get_recover.probability = 1.0;
  get_recover.action.kind = FaultInjectionAction::Kind::kHttp;
  get_recover.action.status_code = 200;
  get_recover.action.body =
      R"({"uuid":"d1-uuid","identifier":"AUTOBOT-D1","market":"KRW-BTC","side":"bid","ord_type":"limit","state":"wait","price":"100000000","volume":"0.01","executed_volume":"0","remaining_volume":"0.01"})";
  get_recover.action.headers = {{"remaining-req", "group=order; min=1800; sec=29"}};
  transport->AddRule(get_recover);
  transport->SetDefaultHttp(200, "{}");

  auto http_client = std::make_unique<autobot::executor::upbit::UpbitHttpClient>(
      BuildHttpOptions(),
      std::move(transport));
  UpbitRestClient rest_client(/*order_test_mode=*/false, std::move(http_client));
  {
    OrderManager manager(&rest_client);
    ManagedIntent intent;
    intent.intent_id = "intent-d1";
    intent.identifier = "AUTOBOT-D1";
    intent.market = "KRW-BTC";
    intent.side = "bid";
    intent.ord_type = "limit";
    intent.price = 100000000.0;
    intent.volume = 0.01;
    intent.tif = "gtc";
    intent.ts_ms = NowMs();

    const auto result = manager.SubmitIntent(intent);
    assert(result.accepted);
    assert(result.identifier == intent.identifier);
    assert(transport_ptr->CallCount("POST", "/v1/orders") == 1);
    assert(transport_ptr->CallCount("GET", "/v1/order") == 1);
    assert(DrainUntilEvent(&manager, "ORDER_UPDATE", "ORDER_ACCEPTED", 1000));
  }

  autobot::executor::state::ExecutorStateStore store(state_path.string());
  assert(store.Load());
  const auto record = store.Find("AUTOBOT-D1");
  assert(record.has_value());
  assert(record->status == "CONFIRMED");
  assert(record->upbit_uuid == "d1-uuid");
  CleanupStateFile(state_path);
}

void TestD2Post5xxRecoverByGetIdentifier() {
  using autobot::executor::ManagedIntent;
  using autobot::executor::OrderManager;
  using autobot::executor::UpbitRestClient;
  using autobot::executor::tests::FaultInjectionAction;
  using autobot::executor::tests::FaultInjectionRule;
  using autobot::executor::tests::FaultInjectionTransport;

  const std::filesystem::path state_path = MakeStatePath("d2");
  CleanupStateFile(state_path);
  ConfigureLiveTestEnv(state_path);

  auto transport = std::make_unique<FaultInjectionTransport>();
  FaultInjectionTransport* transport_ptr = transport.get();
  FaultInjectionRule post_500;
  post_500.method = "POST";
  post_500.endpoint = "/v1/orders";
  post_500.nth_call = 1;
  post_500.probability = 1.0;
  post_500.action.kind = FaultInjectionAction::Kind::kHttp;
  post_500.action.status_code = 500;
  post_500.action.body = R"({"error":{"name":"server_error","message":"injected_500"}})";
  transport->AddRule(post_500);

  FaultInjectionRule get_recover;
  get_recover.method = "GET";
  get_recover.endpoint = "/v1/order";
  get_recover.nth_call = 1;
  get_recover.probability = 1.0;
  get_recover.action.kind = FaultInjectionAction::Kind::kHttp;
  get_recover.action.status_code = 200;
  get_recover.action.body =
      R"({"uuid":"d2-uuid","identifier":"AUTOBOT-D2","market":"KRW-BTC","side":"bid","ord_type":"limit","state":"wait","price":"100000000","volume":"0.01","executed_volume":"0","remaining_volume":"0.01"})";
  transport->AddRule(get_recover);
  transport->SetDefaultHttp(200, "{}");

  auto http_client = std::make_unique<autobot::executor::upbit::UpbitHttpClient>(
      BuildHttpOptions(),
      std::move(transport));
  UpbitRestClient rest_client(/*order_test_mode=*/false, std::move(http_client));
  {
    OrderManager manager(&rest_client);
    ManagedIntent intent;
    intent.intent_id = "intent-d2";
    intent.identifier = "AUTOBOT-D2";
    intent.market = "KRW-BTC";
    intent.side = "bid";
    intent.ord_type = "limit";
    intent.price = 100000000.0;
    intent.volume = 0.01;
    intent.tif = "gtc";
    intent.ts_ms = NowMs();

    const auto result = manager.SubmitIntent(intent);
    assert(result.accepted);
    assert(result.identifier == intent.identifier);
    assert(transport_ptr->CallCount("POST", "/v1/orders") == 1);
    assert(transport_ptr->CallCount("GET", "/v1/order") == 1);
    assert(DrainUntilEvent(&manager, "ORDER_UPDATE", "ORDER_ACCEPTED", 1000));
  }

  autobot::executor::state::ExecutorStateStore store(state_path.string());
  assert(store.Load());
  const auto record = store.Find("AUTOBOT-D2");
  assert(record.has_value());
  assert(record->status == "CONFIRMED");
  assert(record->upbit_uuid == "d2-uuid");
  CleanupStateFile(state_path);
}

void TestD3GroupBreakerOn429() {
  using autobot::executor::tests::FaultInjectionAction;
  using autobot::executor::tests::FaultInjectionRule;
  using autobot::executor::tests::FaultInjectionTransport;

  auto transport = std::make_unique<FaultInjectionTransport>();
  FaultInjectionTransport* transport_ptr = transport.get();
  FaultInjectionRule first_429;
  first_429.method = "GET";
  first_429.endpoint = "/v1/accounts";
  first_429.nth_call = 1;
  first_429.probability = 1.0;
  first_429.action.kind = FaultInjectionAction::Kind::kHttp;
  first_429.action.status_code = 429;
  first_429.action.body = R"({"error":{"name":"too_many_requests","message":"injected_429"}})";
  transport->AddRule(first_429);
  transport->SetDefaultHttp(200, "[]");

  auto options = BuildHttpOptions();
  options.max_attempts = 1;
  autobot::executor::upbit::UpbitHttpClient http_client(options, std::move(transport));

  autobot::executor::upbit::HttpRequest req;
  req.method = "GET";
  req.endpoint = "/v1/accounts";
  req.allow_retry = true;
  req.rate_limit_group = "order";
  req.auth = false;

  const auto first = http_client.RequestJson(req);
  assert(!first.ok);
  assert(first.status_code == 429);
  assert(first.breaker_state == "group");

  const auto started = std::chrono::steady_clock::now();
  const auto second = http_client.RequestJson(req);
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - started)
                              .count();
  assert(second.ok);
  assert(elapsed_ms >= 900);
  assert(transport_ptr->CallCount("GET", "/v1/accounts") == 2);
}

void TestD4GlobalBreakerOn418() {
  using autobot::executor::tests::FaultInjectionAction;
  using autobot::executor::tests::FaultInjectionRule;
  using autobot::executor::tests::FaultInjectionTransport;

  auto transport = std::make_unique<FaultInjectionTransport>();
  FaultInjectionTransport* transport_ptr = transport.get();
  FaultInjectionRule first_418;
  first_418.method = "GET";
  first_418.endpoint = "/v1/accounts";
  first_418.nth_call = 1;
  first_418.probability = 1.0;
  first_418.action.kind = FaultInjectionAction::Kind::kHttp;
  first_418.action.status_code = 418;
  first_418.action.body = R"({"error":{"name":"too_many_requests","message":"blocked for 2 seconds"}})";
  transport->AddRule(first_418);
  transport->SetDefaultHttp(200, "{}");

  auto options = BuildHttpOptions();
  options.max_attempts = 1;
  options.ban_cooldown_sec = 1;
  autobot::executor::upbit::UpbitHttpClient http_client(options, std::move(transport));

  autobot::executor::upbit::HttpRequest first_req;
  first_req.method = "GET";
  first_req.endpoint = "/v1/accounts";
  first_req.rate_limit_group = "order";

  autobot::executor::upbit::HttpRequest second_req;
  second_req.method = "GET";
  second_req.endpoint = "/v1/order";
  second_req.rate_limit_group = "default";

  const auto first = http_client.RequestJson(first_req);
  assert(!first.ok);
  assert(first.status_code == 418);
  assert(first.breaker_state == "global");

  const auto started = std::chrono::steady_clock::now();
  const auto second = http_client.RequestJson(second_req);
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - started)
                              .count();
  assert(second.ok);
  assert(elapsed_ms >= 1900);
  assert(transport_ptr->CallCount("GET", "/v1/order") == 1);
}

class FakeFlakyWsClient final : public autobot::executor::upbit::UpbitPrivateWsClient {
 public:
  explicit FakeFlakyWsClient(const autobot::executor::upbit::WsPrivateClientOptions& options)
      : UpbitPrivateWsClient(options) {}

  void Run(
      const std::function<std::string()>& authorization_header_provider,
      const std::atomic<bool>* stop_flag,
      const Callbacks& callbacks) override {
    if (authorization_header_provider && authorization_header_provider().empty()) {
      if (callbacks.on_error) {
        callbacks.on_error("authorization_header_missing");
      }
      return;
    }

    if (callbacks.on_connected) {
      callbacks.on_connected();
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stats_.last_connect_ts_ms = NowMs();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    if (stop_flag != nullptr && stop_flag->load()) {
      if (callbacks.on_disconnected) {
        callbacks.on_disconnected("stopped");
      }
      return;
    }

    if (callbacks.on_disconnected) {
      callbacks.on_disconnected("injected_ws_drop");
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stats_.reconnect_count = 1;
      stats_.last_disconnect_reason = "injected_ws_drop";
      stats_.last_disconnect_ts_ms = NowMs();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    if (stop_flag != nullptr && stop_flag->load()) {
      if (callbacks.on_disconnected) {
        callbacks.on_disconnected("stopped");
      }
      return;
    }

    if (callbacks.on_connected) {
      callbacks.on_connected();
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stats_.last_connect_ts_ms = NowMs();
    }

    while (stop_flag == nullptr || !stop_flag->load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    if (callbacks.on_disconnected) {
      callbacks.on_disconnected("stopped");
    }
  }

  autobot::executor::upbit::WsPrivateClientStats Stats() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
  }

 private:
  mutable std::mutex mutex_;
  autobot::executor::upbit::WsPrivateClientStats stats_;
};

void TestD5WsDropReconnectAndRestFallback() {
  using autobot::executor::OrderManager;
  using autobot::executor::UpbitRestClient;
  using autobot::executor::tests::FaultInjectionRule;
  using autobot::executor::tests::FaultInjectionTransport;

  const std::filesystem::path state_path = MakeStatePath("d5");
  CleanupStateFile(state_path);
  ConfigureLiveTestEnv(state_path);

  SetEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLED", "true");
  SetEnv("AUTOBOT_EXECUTOR_POLL_INTERVAL_WS_CONNECTED_SEC", "4");
  SetEnv("AUTOBOT_EXECUTOR_POLL_INTERVAL_WS_DEGRADED_SEC", "1");
  SetEnv("AUTOBOT_EXECUTOR_POLL_INTERVAL_REST_ONLY_MS", "1500");

  auto transport = std::make_unique<FaultInjectionTransport>();
  FaultInjectionRule accounts_ok;
  accounts_ok.method = "GET";
  accounts_ok.endpoint = "/v1/accounts";
  accounts_ok.nth_call = 1;
  accounts_ok.probability = 1.0;
  accounts_ok.action.kind = autobot::executor::tests::FaultInjectionAction::Kind::kHttp;
  accounts_ok.action.status_code = 200;
  accounts_ok.action.body = "[]";
  transport->AddRule(accounts_ok);
  transport->SetDefaultHttp(200, "{}");

  auto http_client = std::make_unique<autobot::executor::upbit::UpbitHttpClient>(
      BuildHttpOptions(),
      std::move(transport));
  UpbitRestClient rest_client(/*order_test_mode=*/false, std::move(http_client));

  auto ws_factory =
      [](const autobot::executor::upbit::WsPrivateClientOptions& options)
      -> std::unique_ptr<autobot::executor::upbit::UpbitPrivateWsClient> {
    return std::make_unique<FakeFlakyWsClient>(options);
  };

  {
    OrderManager manager(&rest_client, ws_factory);

    const bool saw_connected = WaitUntil(
        [&manager]() {
          const auto snapshot = manager.Snapshot();
          return snapshot["ws_private"].value("connected", false) &&
                 snapshot["ws_private"].value("current_poll_interval_ms", 0) == 4000;
        },
        1500);
    assert(saw_connected);

    const bool saw_degraded = WaitUntil(
        [&manager]() {
          const auto snapshot = manager.Snapshot();
          return !snapshot["ws_private"].value("connected", true) &&
                 snapshot["ws_private"].value("current_poll_interval_ms", 0) == 1000;
        },
        2500);
    assert(saw_degraded);

    const bool recovered_connected = WaitUntil(
        [&manager]() {
          const auto snapshot = manager.Snapshot();
          return snapshot["ws_private"].value("connected", false) &&
                 snapshot["ws_private"].value("current_poll_interval_ms", 0) == 4000;
        },
        2500);
    assert(recovered_connected);

    const bool saw_disconnect_error = WaitUntil(
        [&manager]() {
          autobot::executor::ManagedEvent event;
          if (!manager.PopEvent(&event, std::chrono::milliseconds(30))) {
            return false;
          }
          if (event.event_type != "ERROR") {
            return false;
          }
          if (event.payload.value("where", "") != "order_manager.private_ws.disconnect") {
            return false;
          }
          return event.payload.value("upbit_error_message", "") == "injected_ws_drop";
        },
        2000);
    assert(saw_disconnect_error);
  }

  CleanupStateFile(state_path);
}

void TestD6SubmitRejectedByExactAdmissibilityBeforePost() {
  using autobot::executor::ManagedIntent;
  using autobot::executor::OrderManager;
  using autobot::executor::UpbitRestClient;
  using autobot::executor::tests::FaultInjectionAction;
  using autobot::executor::tests::FaultInjectionRule;
  using autobot::executor::tests::FaultInjectionTransport;

  const std::filesystem::path state_path = MakeStatePath("d6");
  CleanupStateFile(state_path);
  ConfigureLiveTestEnv(state_path);

  auto transport = std::make_unique<FaultInjectionTransport>();

  FaultInjectionRule chance_ok;
  chance_ok.method = "GET";
  chance_ok.endpoint = "/v1/orders/chance";
  chance_ok.nth_call = 1;
  chance_ok.probability = 1.0;
  chance_ok.action.kind = FaultInjectionAction::Kind::kHttp;
  chance_ok.action.status_code = 200;
  chance_ok.action.body =
      R"({"bid_fee":"0.0005","ask_fee":"0.0005","market":{"bid":{"min_total":"5000"},"ask":{"min_total":"5000"}}})";
  transport->AddRule(chance_ok);

  FaultInjectionRule accounts_ok;
  accounts_ok.method = "GET";
  accounts_ok.endpoint = "/v1/accounts";
  accounts_ok.nth_call = 1;
  accounts_ok.probability = 1.0;
  accounts_ok.action.kind = FaultInjectionAction::Kind::kHttp;
  accounts_ok.action.status_code = 200;
  accounts_ok.action.body =
      R"([{"currency":"KRW","balance":"5000","locked":"0","avg_buy_price":"1"},{"currency":"BTC","balance":"0.1","locked":"0","avg_buy_price":"100000000"}])";
  transport->AddRule(accounts_ok);

  FaultInjectionRule instruments_ok;
  instruments_ok.method = "GET";
  instruments_ok.endpoint = "/v1/orderbook/instruments";
  instruments_ok.nth_call = 1;
  instruments_ok.probability = 1.0;
  instruments_ok.action.kind = FaultInjectionAction::Kind::kHttp;
  instruments_ok.action.status_code = 200;
  instruments_ok.action.body = R"([{"market":"KRW-BTC","tick_size":"1000"}])";
  transport->AddRule(instruments_ok);

  transport->SetDefaultHttp(200, "{}");

  auto http_client = std::make_unique<autobot::executor::upbit::UpbitHttpClient>(
      BuildHttpOptions(),
      std::move(transport));
  UpbitRestClient rest_client(/*order_test_mode=*/false, std::move(http_client));
  {
    OrderManager manager(&rest_client);
    ManagedIntent intent;
    intent.intent_id = "intent-d6";
    intent.identifier = "AUTOBOT-D6";
    intent.market = "KRW-BTC";
    intent.side = "bid";
    intent.ord_type = "limit";
    intent.price = 5000.0;
    intent.volume = 1.0;
    intent.tif = "gtc";
    intent.ts_ms = NowMs();

    const auto result = manager.SubmitIntent(intent);
    assert(!result.accepted);
    assert(result.reason == "FEE_RESERVE_INSUFFICIENT");
  }

  CleanupStateFile(state_path);
}

void TestD7ReplaceRejectedByExactAdmissibilityBeforeCancelAndNew() {
  using autobot::executor::UpbitRestClient;
  using autobot::executor::UpbitReplaceRequest;
  using autobot::executor::tests::FaultInjectionAction;
  using autobot::executor::tests::FaultInjectionRule;
  using autobot::executor::tests::FaultInjectionTransport;

  const std::filesystem::path state_path = MakeStatePath("d7");
  CleanupStateFile(state_path);
  ConfigureLiveTestEnv(state_path);

  auto transport = std::make_unique<FaultInjectionTransport>();

  FaultInjectionRule get_order_ok;
  get_order_ok.method = "GET";
  get_order_ok.endpoint = "/v1/order";
  get_order_ok.nth_call = 1;
  get_order_ok.probability = 1.0;
  get_order_ok.action.kind = FaultInjectionAction::Kind::kHttp;
  get_order_ok.action.status_code = 200;
  get_order_ok.action.body =
      R"({"uuid":"prev-uuid","identifier":"AUTOBOT-PREV","market":"KRW-BTC","side":"ask","ord_type":"limit","state":"wait","price":"5000","volume":"2","executed_volume":"0","remaining_volume":"2"})";
  transport->AddRule(get_order_ok);

  FaultInjectionRule chance_ok;
  chance_ok.method = "GET";
  chance_ok.endpoint = "/v1/orders/chance";
  chance_ok.nth_call = 1;
  chance_ok.probability = 1.0;
  chance_ok.action.kind = FaultInjectionAction::Kind::kHttp;
  chance_ok.action.status_code = 200;
  chance_ok.action.body =
      R"({"bid_fee":"0.0005","ask_fee":"0.0005","market":{"bid":{"min_total":"5000"},"ask":{"min_total":"5000"}}})";
  transport->AddRule(chance_ok);

  FaultInjectionRule accounts_ok;
  accounts_ok.method = "GET";
  accounts_ok.endpoint = "/v1/accounts";
  accounts_ok.nth_call = 1;
  accounts_ok.probability = 1.0;
  accounts_ok.action.kind = FaultInjectionAction::Kind::kHttp;
  accounts_ok.action.status_code = 200;
  accounts_ok.action.body =
      R"([{"currency":"KRW","balance":"14700","locked":"0","avg_buy_price":"1"},{"currency":"BTC","balance":"2","locked":"0","avg_buy_price":"100000000"}])";
  transport->AddRule(accounts_ok);

  FaultInjectionRule instruments_ok;
  instruments_ok.method = "GET";
  instruments_ok.endpoint = "/v1/orderbook/instruments";
  instruments_ok.nth_call = 1;
  instruments_ok.probability = 1.0;
  instruments_ok.action.kind = FaultInjectionAction::Kind::kHttp;
  instruments_ok.action.status_code = 200;
  instruments_ok.action.body = R"([{"market":"KRW-BTC","tick_size":"1000"}])";
  transport->AddRule(instruments_ok);

  transport->SetDefaultHttp(200, "{}");

  auto http_client = std::make_unique<autobot::executor::upbit::UpbitHttpClient>(
      BuildHttpOptions(),
      std::move(transport));
  UpbitRestClient rest_client(/*order_test_mode=*/false, std::move(http_client));

  UpbitReplaceRequest request;
  request.intent_id = "replace-d7";
  request.prev_order_uuid = "prev-uuid";
  request.prev_order_identifier = "AUTOBOT-PREV";
  request.new_identifier = "AUTOBOT-NEW";
  request.new_price_str = "5000";
  request.new_volume_str = "1.5";
  request.new_time_in_force = "gtc";

  const auto result = rest_client.ReplaceOrder(request);
  assert(!result.accepted);
  assert(result.reason == "DUST_REMAINDER");

  CleanupStateFile(state_path);
}

}  // namespace

int main() {
  TestD1PostTimeoutRecoverByGetIdentifier();
  TestD2Post5xxRecoverByGetIdentifier();
  TestD3GroupBreakerOn429();
  TestD4GlobalBreakerOn418();
  TestD5WsDropReconnectAndRestFallback();
  TestD6SubmitRejectedByExactAdmissibilityBeforePost();
  TestD7ReplaceRejectedByExactAdmissibilityBeforeCancelAndNew();
  return 0;
}
