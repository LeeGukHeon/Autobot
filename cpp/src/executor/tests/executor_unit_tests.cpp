#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "state/executor_state_store.h"
#include "upbit/number_string.h"
#include "upbit/recovery_policy.h"
#include "upbit/request_builder.h"
#include "upbit/upbit_private.h"
#include "upbit_rest.h"

namespace {

void SetEnv(const char* key, const char* value) {
#ifdef _WIN32
  _putenv_s(key, value);
#else
  setenv(key, value, 1);
#endif
}

void TestQueryHashContract() {
  using autobot::executor::upbit::BuildUnencodedQueryString;
  using autobot::executor::upbit::BuildUrlEncodedQueryString;
  using autobot::executor::upbit::OrderedParams;

  const OrderedParams params = {
      {"market", "KRW-BTC"},
      {"states[]", "wait"},
      {"states[]", "watch"},
  };
  const std::string hash_query = BuildUnencodedQueryString(params);
  assert(hash_query == "market=KRW-BTC&states[]=wait&states[]=watch");

  const std::string url_query = BuildUrlEncodedQueryString(params);
  assert(url_query.find("states[]") != std::string::npos);

  const OrderedParams special = {
      {"query", "A B/C"},
  };
  const std::string special_hash_query = BuildUnencodedQueryString(special);
  assert(special_hash_query == "query=A B/C");
  const std::string special_url_query = BuildUrlEncodedQueryString(special);
  assert(special_url_query == "query=A+B%2FC");
}

void TestPostBodyHashConsistency() {
  using autobot::executor::upbit::BuildPreparedRequest;
  using autobot::executor::upbit::RequestSpec;

  RequestSpec spec;
  spec.method = "POST";
  spec.path = "/v1/orders";
  spec.body_params = {
      {"market", "KRW-BTC"},
      {"side", "bid"},
      {"volume", "0.01"},
      {"price", "100000000"},
      {"ord_type", "limit"},
  };

  const auto prepared = BuildPreparedRequest(spec);
  assert(prepared.query_string_for_hash == "market=KRW-BTC&side=bid&volume=0.01&price=100000000&ord_type=limit");
  assert(prepared.body_json.find("\"market\":\"KRW-BTC\"") != std::string::npos);
  assert(prepared.body_json.find("\"volume\":\"0.01\"") != std::string::npos);
}

void TestRecoveryPolicy() {
  using autobot::executor::upbit::DecideCreateOrderRecovery;
  using autobot::executor::upbit::HttpResponse;
  using autobot::executor::upbit::RecoveryAction;

  HttpResponse timeout;
  timeout.ok = false;
  timeout.status_code = 0;
  timeout.category = "network";
  timeout.retriable = true;
  auto timeout_decision = DecideCreateOrderRecovery(timeout);
  assert(timeout_decision.action == RecoveryAction::kRecoverByGetIdentifier);

  HttpResponse rate_429;
  rate_429.ok = false;
  rate_429.status_code = 429;
  auto rate_429_decision = DecideCreateOrderRecovery(rate_429);
  assert(rate_429_decision.action == RecoveryAction::kFail);
}

void TestOrderTestCancelGuard() {
  SetEnv("AUTOBOT_EXECUTOR_STATE_PATH", "data/state/executor_state_unit_test.json");
  autobot::executor::UpbitRestClient rest_client(/*order_test_mode=*/true);
  autobot::executor::UpbitCancelRequest request;
  request.identifier = "AUTOBOT-UNIT-TEST-CANCEL";

  const autobot::executor::UpbitCancelResult result = rest_client.CancelOrder(request);
  assert(result.accepted);
  assert(result.reason.find("order_test_mode") != std::string::npos);
}

void TestOrdersUuidsConstraints() {
  autobot::executor::upbit::HttpClientOptions options;
  options.max_attempts = 1;
  autobot::executor::upbit::UpbitHttpClient http_client(options);
  autobot::executor::upbit::UpbitPrivateClient private_client(&http_client);

  const auto mixed = private_client.CancelOrdersByKeys({"uuid-1"}, {"id-1"});
  assert(!mixed.ok);
  assert(mixed.status_code == 400);

  std::vector<std::string> too_many(21, "uuid");
  const auto overflow = private_client.CancelOrdersByKeys(too_many, {});
  assert(!overflow.ok);
  assert(overflow.status_code == 400);
}

void TestCancelAndNewConstraints() {
  autobot::executor::upbit::HttpClientOptions options;
  options.max_attempts = 1;
  autobot::executor::upbit::UpbitHttpClient http_client(options);
  autobot::executor::upbit::UpbitPrivateClient private_client(&http_client);

  autobot::executor::upbit::CancelAndNewRequest missing_prev;
  missing_prev.new_identifier = "AUTOBOT-REPLACE-1";
  missing_prev.new_price = "1000";
  missing_prev.new_volume = "0.01";
  const auto missing_prev_result = private_client.CancelAndNewOrder(missing_prev);
  assert(!missing_prev_result.ok);
  assert(missing_prev_result.status_code == 400);

  autobot::executor::upbit::CancelAndNewRequest invalid_volume;
  invalid_volume.prev_order_uuid = "uuid-1";
  invalid_volume.new_identifier = "AUTOBOT-REPLACE-2";
  invalid_volume.new_price = "1000";
  invalid_volume.new_volume = "not-a-number";
  const auto invalid_volume_result = private_client.CancelAndNewOrder(invalid_volume);
  assert(!invalid_volume_result.ok);
  assert(invalid_volume_result.status_code == 400);
}

void TestReplaceOrderOrderTestModeGuard() {
  SetEnv("AUTOBOT_EXECUTOR_STATE_PATH", "data/state/executor_state_unit_test.json");
  autobot::executor::UpbitRestClient rest_client(/*order_test_mode=*/true);
  autobot::executor::UpbitReplaceRequest request;
  request.intent_id = "intent-1";
  request.prev_order_uuid = "uuid-1";
  request.new_identifier = "AUTOBOT-REPLACE-ORDERTEST-1";
  request.new_price_str = "1000";
  request.new_volume_str = "0.01";

  const autobot::executor::UpbitReplaceResult result = rest_client.ReplaceOrder(request);
  assert(!result.accepted);
  assert(result.reason.find("order_test_mode") != std::string::npos);
}

void TestNumberStringNormalization() {
  using autobot::executor::upbit::FormatNumberString;

  assert(FormatNumberString(-0.0, 8) == "0");
  assert(FormatNumberString(-0.00001, 4) == "0");
  assert(FormatNumberString(123.0, 4) == "123");
  assert(FormatNumberString(123.4500, 4) == "123.45");
}

void TestStateStoreForceUnlockStaleLock() {
  using autobot::executor::state::ExecutorStateStore;
  using autobot::executor::state::IdentifierStateRecord;
  namespace fs = std::filesystem;

  const fs::path state_path = fs::path("data") / "state" / "executor_state_stale_lock_unit_test.json";
  const fs::path lock_path = fs::path(state_path.string() + ".lock");
  const fs::path lock_owner_path = lock_path / "owner.json";
  const fs::path backup_path = fs::path(state_path.string() + ".bak");
  const fs::path tmp_path = fs::path(state_path.string() + ".tmp");

  std::error_code ec;
  fs::create_directories(state_path.parent_path(), ec);
  ec.clear();
  fs::remove(state_path, ec);
  ec.clear();
  fs::remove(backup_path, ec);
  ec.clear();
  fs::remove(tmp_path, ec);
  ec.clear();
  fs::remove_all(lock_path, ec);
  ec.clear();

  fs::create_directories(lock_path, ec);
  assert(!ec);
  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  const auto stale_created_ms = now_ms - (11 * 60 * 1000);
  {
    std::ofstream lock_owner(lock_owner_path, std::ios::binary | std::ios::trunc);
    assert(lock_owner.good());
    lock_owner << "{\"owner_pid\":999999,\"created_at_ms\":" << stale_created_ms << "}";
    lock_owner.flush();
    assert(lock_owner.good());
  }

  SetEnv("AUTOBOT_EXECUTOR_FORCE_UNLOCK", "YES");
  ExecutorStateStore store(state_path.string());
  IdentifierStateRecord record;
  record.identifier = "AUTOBOT-UNIT-TEST-STALE-LOCK";
  record.status = "CONFIRMED";
  record.created_at_ms = now_ms;
  record.updated_at_ms = now_ms;
  store.Upsert(record);
  assert(store.Save());

  ExecutorStateStore reloaded(state_path.string());
  assert(reloaded.Load());
  const auto loaded = reloaded.Find(record.identifier);
  assert(loaded.has_value());

  SetEnv("AUTOBOT_EXECUTOR_FORCE_UNLOCK", "");
  fs::remove(state_path, ec);
  ec.clear();
  fs::remove(backup_path, ec);
  ec.clear();
  fs::remove(tmp_path, ec);
  ec.clear();
  fs::remove_all(lock_path, ec);
}

}  // namespace

int main() {
  TestQueryHashContract();
  TestPostBodyHashConsistency();
  TestRecoveryPolicy();
  TestOrderTestCancelGuard();
  TestOrdersUuidsConstraints();
  TestCancelAndNewConstraints();
  TestReplaceOrderOrderTestModeGuard();
  TestNumberStringNormalization();
  TestStateStoreForceUnlockStaleLock();
  return 0;
}
