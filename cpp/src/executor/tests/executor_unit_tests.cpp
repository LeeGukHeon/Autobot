#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "state/executor_state_store.h"
#include "upbit/auth_jwt.h"
#include "upbit/number_string.h"
#include "upbit/recovery_policy.h"
#include "upbit/request_builder.h"
#include "upbit/tif_policy.h"
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
  assert(special_hash_query == "query=A+B/C");
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
  assert(prepared.url_query.empty());
  assert(prepared.body_json.find("\"market\":\"KRW-BTC\"") != std::string::npos);
  assert(prepared.body_json.find("\"volume\":\"0.01\"") != std::string::npos);
}

void TestPostBodyHashGoldenVector() {
  using autobot::executor::upbit::BuildPreparedRequest;
  using autobot::executor::upbit::RequestSpec;
  using autobot::executor::upbit::UpbitJwtSigner;

  RequestSpec spec;
  spec.method = "POST";
  spec.path = "/v1/orders/test";
  spec.query_params = {
      {"market", "SHOULD-NOT-BE-ON-POST-URL"},
  };
  spec.body_params = {
      {"market", "KRW-BTC"},
      {"side", "bid"},
      {"volume", "1"},
      {"price", "140000000"},
      {"ord_type", "limit"},
      {"identifier", ""},
  };

  const auto prepared = BuildPreparedRequest(spec);
  assert(prepared.url_query.empty());
  assert(prepared.query_string_for_hash ==
         "market=KRW-BTC&side=bid&volume=1&price=140000000&ord_type=limit");
  assert(prepared.body_json.find("\"identifier\"") == std::string::npos);

  const std::size_t market_pos = prepared.body_json.find("\"market\"");
  const std::size_t side_pos = prepared.body_json.find("\"side\"");
  const std::size_t volume_pos = prepared.body_json.find("\"volume\"");
  const std::size_t price_pos = prepared.body_json.find("\"price\"");
  const std::size_t ord_type_pos = prepared.body_json.find("\"ord_type\"");
  assert(market_pos != std::string::npos);
  assert(side_pos != std::string::npos);
  assert(volume_pos != std::string::npos);
  assert(price_pos != std::string::npos);
  assert(ord_type_pos != std::string::npos);
  assert(market_pos < side_pos);
  assert(side_pos < volume_pos);
  assert(volume_pos < price_pos);
  assert(price_pos < ord_type_pos);

  const std::string query_hash = UpbitJwtSigner::HashQueryString(prepared.query_string_for_hash);
  assert(query_hash ==
         "5017abae1487d9a07531c830df42357630c73e0e7bbd8bd65678a5ecfc9ee3390e4d35878fbc8d3c01e6d7c9ebd2b3df044b459b3502585157c4553deca42c97");
  assert(query_hash.size() == 128);
}

void TestTimeInForceLimitGtcIsOmitted() {
  using autobot::executor::upbit::BuildPreparedRequest;
  using autobot::executor::upbit::NormalizeTimeInForce;
  using autobot::executor::upbit::RequestSpec;

  std::string validation_error;
  const auto tif = NormalizeTimeInForce("limit", "gtc", &validation_error);
  assert(validation_error.empty());
  assert(!tif.has_value());

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
  if (tif.has_value()) {
    spec.body_params.emplace_back("time_in_force", *tif);
  }

  const auto prepared = BuildPreparedRequest(spec);
  assert(prepared.query_string_for_hash.find("time_in_force") == std::string::npos);
  assert(prepared.body_json.find("\"time_in_force\"") == std::string::npos);
}

void TestTimeInForceLimitIocIsIncluded() {
  using autobot::executor::upbit::BuildPreparedRequest;
  using autobot::executor::upbit::NormalizeTimeInForce;
  using autobot::executor::upbit::RequestSpec;

  std::string validation_error;
  const auto tif = NormalizeTimeInForce("limit", "ioc", &validation_error);
  assert(validation_error.empty());
  assert(tif.has_value());
  assert(*tif == "ioc");

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
  if (tif.has_value()) {
    spec.body_params.emplace_back("time_in_force", *tif);
  }

  const auto prepared = BuildPreparedRequest(spec);
  assert(prepared.query_string_for_hash.find("time_in_force=ioc") != std::string::npos);
  assert(prepared.body_json.find("\"time_in_force\":\"ioc\"") != std::string::npos);
}

void TestTimeInForceBestRequiresIocOrFok() {
  using autobot::executor::upbit::NormalizeTimeInForce;

  std::string validation_error;
  const auto missing = NormalizeTimeInForce("best", "", &validation_error);
  assert(!missing.has_value());
  assert(!validation_error.empty());
  assert(validation_error.find("required") != std::string::npos);

  validation_error.clear();
  const auto gtc = NormalizeTimeInForce("best", "gtc", &validation_error);
  assert(!gtc.has_value());
  assert(!validation_error.empty());
  assert(validation_error.find("required") != std::string::npos);
}

void TestReplaceTimeInForceLimitGtcIsOmitted() {
  using autobot::executor::upbit::BuildPreparedRequest;
  using autobot::executor::upbit::NormalizeTimeInForce;
  using autobot::executor::upbit::RequestSpec;

  std::string validation_error;
  const auto tif = NormalizeTimeInForce("limit", "gtc", &validation_error);
  assert(validation_error.empty());
  assert(!tif.has_value());

  RequestSpec spec;
  spec.method = "POST";
  spec.path = "/v1/orders/cancel_and_new";
  spec.body_params = {
      {"prev_order_uuid", "prev-uuid-1"},
      {"new_identifier", "AUTOBOT-REPLACE-1"},
      {"new_ord_type", "limit"},
      {"new_price", "1000"},
      {"new_volume", "0.01"},
  };
  if (tif.has_value()) {
    spec.body_params.emplace_back("new_time_in_force", *tif);
  }

  const auto prepared = BuildPreparedRequest(spec);
  assert(prepared.query_string_for_hash.find("new_time_in_force") == std::string::npos);
  assert(prepared.body_json.find("\"new_time_in_force\"") == std::string::npos);
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
  record.upbit_uuid = "unit-test-uuid-1";
  record.prev_identifier = "AUTOBOT-UNIT-TEST-ROOT";
  record.prev_upbit_uuid = "unit-test-uuid-0";
  record.root_identifier = "AUTOBOT-UNIT-TEST-ROOT";
  record.root_upbit_uuid = "unit-test-uuid-0";
  record.chain_status = "REPLACE_CONFIRMED";
  record.replace_attempt = 2;
  record.last_replace_ts_ms = now_ms - 1234;
  record.status = "CONFIRMED";
  record.created_at_ms = now_ms;
  record.updated_at_ms = now_ms;
  store.Upsert(record);
  assert(store.Save());

  ExecutorStateStore reloaded(state_path.string());
  assert(reloaded.Load());
  const auto loaded = reloaded.Find(record.identifier);
  assert(loaded.has_value());
  assert(loaded->upbit_uuid == record.upbit_uuid);
  assert(loaded->prev_identifier == record.prev_identifier);
  assert(loaded->prev_upbit_uuid == record.prev_upbit_uuid);
  assert(loaded->root_identifier == record.root_identifier);
  assert(loaded->root_upbit_uuid == record.root_upbit_uuid);
  assert(loaded->chain_status == record.chain_status);
  assert(loaded->replace_attempt == record.replace_attempt);
  assert(loaded->last_replace_ts_ms == record.last_replace_ts_ms);

  const auto loaded_by_uuid = reloaded.FindByUpbitUuid(record.upbit_uuid);
  assert(loaded_by_uuid.has_value());
  assert(loaded_by_uuid->identifier == record.identifier);

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
  TestPostBodyHashGoldenVector();
  TestTimeInForceLimitGtcIsOmitted();
  TestTimeInForceLimitIocIsIncluded();
  TestTimeInForceBestRequiresIocOrFok();
  TestReplaceTimeInForceLimitGtcIsOmitted();
  TestRecoveryPolicy();
  TestOrderTestCancelGuard();
  TestOrdersUuidsConstraints();
  TestCancelAndNewConstraints();
  TestReplaceOrderOrderTestModeGuard();
  TestNumberStringNormalization();
  TestStateStoreForceUnlockStaleLock();
  return 0;
}
