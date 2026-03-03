#include "upbit_rest.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cmath>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "upbit/number_string.h"
#include "upbit/recovery_policy.h"
#include "upbit/tif_policy.h"

namespace autobot::executor {

namespace {

std::string Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

std::string ToUpper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  return value;
}

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string EnvOrDefault(const char* key, const std::string& default_value) {
  const char* raw = std::getenv(key);
  if (raw == nullptr) {
    return default_value;
  }
  const std::string value = Trim(raw);
  if (value.empty()) {
    return default_value;
  }
  return value;
}

std::string EnvRequired(const char* key) {
  const char* raw = std::getenv(key);
  if (raw == nullptr) {
    return "";
  }
  return Trim(raw);
}

std::int64_t NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

double ParsePositiveDoubleEnv(const char* key, double fallback) {
  const char* raw = std::getenv(key);
  if (raw == nullptr) {
    return fallback;
  }
  try {
    return std::max(std::stod(Trim(raw)), 0.0);
  } catch (...) {
    return fallback;
  }
}

std::unordered_set<std::string> ParseCsvUpperSet(const std::string& raw) {
  std::unordered_set<std::string> out;
  std::stringstream stream(raw);
  std::string token;
  while (std::getline(stream, token, ',')) {
    token = ToUpper(Trim(token));
    if (!token.empty()) {
      out.insert(token);
    }
  }
  return out;
}

std::vector<std::string> ParseCsvUpperVector(const std::string& raw) {
  std::vector<std::string> out;
  std::unordered_set<std::string> seen;
  std::stringstream stream(raw);
  std::string token;
  while (std::getline(stream, token, ',')) {
    token = ToUpper(Trim(token));
    if (token.empty()) {
      continue;
    }
    if (seen.insert(token).second) {
      out.push_back(std::move(token));
    }
  }
  return out;
}

bool ParseBoolEnv(const char* key, bool fallback) {
  const char* raw = std::getenv(key);
  if (raw == nullptr) {
    return fallback;
  }
  const std::string value = ToLower(Trim(raw));
  if (value.empty()) {
    return fallback;
  }
  if (value == "1" || value == "true" || value == "yes" || value == "y" || value == "on") {
    return true;
  }
  if (value == "0" || value == "false" || value == "no" || value == "n" || value == "off") {
    return false;
  }
  return fallback;
}

bool StartsWith(std::string_view text, std::string_view prefix) {
  if (text.size() < prefix.size()) {
    return false;
  }
  return text.compare(0, prefix.size(), prefix) == 0;
}

std::string BuildErrorReason(const upbit::HttpResponse& response, const std::string& fallback) {
  if (!response.error_message.empty()) {
    return response.error_message;
  }
  if (!response.error_name.empty()) {
    return response.error_name;
  }
  if (response.status_code > 0) {
    return "HTTP " + std::to_string(response.status_code);
  }
  return fallback;
}

void ApplyRemainingReqToSubmitResult(UpbitSubmitResult* result, const upbit::HttpResponse& response) {
  if (result == nullptr) {
    return;
  }
  if (response.remaining_req.valid) {
    result->remaining_req_group = response.remaining_req.group;
    result->remaining_req_sec = response.remaining_req.sec;
  } else {
    result->remaining_req_group = "default";
    result->remaining_req_sec = -1;
  }
}

void ApplyRemainingReqToCancelResult(UpbitCancelResult* result, const upbit::HttpResponse& response) {
  if (result == nullptr) {
    return;
  }
  if (response.remaining_req.valid) {
    result->remaining_req_group = response.remaining_req.group;
    result->remaining_req_sec = response.remaining_req.sec;
  } else {
    result->remaining_req_group = "default";
    result->remaining_req_sec = -1;
  }
}

void ApplyHttpMetaToRecord(state::IdentifierStateRecord* record, const upbit::HttpResponse& response) {
  if (record == nullptr) {
    return;
  }
  record->last_http_status = response.status_code;
  record->last_error_name = response.error_name;
  if (response.remaining_req.valid) {
    record->last_remaining_req_group = response.remaining_req.group;
    record->last_remaining_req_sec = response.remaining_req.sec;
  } else {
    record->last_remaining_req_group.clear();
    record->last_remaining_req_sec = -1;
  }
  record->updated_at_ms = NowMs();
}

state::IdentifierStateRecord BuildStateRecord(
    const std::string& identifier,
    const std::string& intent_id,
    const std::string& mode,
    const std::string& status) {
  state::IdentifierStateRecord record;
  record.identifier = identifier;
  record.intent_id = intent_id;
  record.mode = mode;
  record.status = status;
  const std::int64_t now = NowMs();
  record.created_at_ms = now;
  record.updated_at_ms = now;
  record.last_remaining_req_sec = -1;
  record.chain_status = "NONE";
  return record;
}

bool IsPositiveNumberString(const std::string& raw) {
  const std::string value = Trim(raw);
  if (value.empty()) {
    return false;
  }
  try {
    const double parsed = std::stod(value);
    return std::isfinite(parsed) && parsed > 0.0;
  } catch (...) {
    return false;
  }
}

}  // namespace

UpbitRestClient::UpbitRestClient(
    bool order_test_mode,
    std::unique_ptr<upbit::UpbitHttpClient> http_client_override)
    : order_test_mode_(order_test_mode),
      mode_name_(order_test_mode ? "order_test" : "live"),
      state_store_(ResolveStateFilePath()) {
  if (!order_test_mode_) {
    const std::string live_gate = EnvOrDefault("AUTOBOT_LIVE_ENABLE", "");
    if (live_gate != "YES") {
      throw std::runtime_error("live mode requires AUTOBOT_LIVE_ENABLE=YES");
    }
  }

  upbit::HttpClientOptions options;
  options.base_url = EnvOrDefault("AUTOBOT_UPBIT_BASE_URL", "https://api.upbit.com");
  options.connect_timeout_ms = std::max(std::stoi(EnvOrDefault("AUTOBOT_UPBIT_CONNECT_TIMEOUT_MS", "3000")), 100);
  options.read_timeout_ms = std::max(std::stoi(EnvOrDefault("AUTOBOT_UPBIT_READ_TIMEOUT_MS", "10000")), 100);
  options.write_timeout_ms = std::max(std::stoi(EnvOrDefault("AUTOBOT_UPBIT_WRITE_TIMEOUT_MS", "10000")), 100);
  options.max_attempts = std::max(std::stoi(EnvOrDefault("AUTOBOT_UPBIT_MAX_ATTEMPTS", "3")), 1);
  options.base_backoff_ms = std::max(std::stoi(EnvOrDefault("AUTOBOT_UPBIT_BASE_BACKOFF_MS", "200")), 1);
  options.max_backoff_ms = std::max(std::stoi(EnvOrDefault("AUTOBOT_UPBIT_MAX_BACKOFF_MS", "2000")), 1);
  options.rate_limit_enabled = EnvOrDefault("AUTOBOT_UPBIT_RATELIMIT_ENABLED", "true") != "false";
  options.ban_cooldown_sec = std::max(std::stoi(EnvOrDefault("AUTOBOT_UPBIT_BAN_COOLDOWN_SEC", "60")), 1);
  options.group_rates = {
      {"default", 30.0},
      {"order", 8.0},
      {"order-test", 8.0},
      {"order-cancel-all", 0.5},
  };
  options.access_key = EnvRequired("UPBIT_ACCESS_KEY");
  options.secret_key = EnvRequired("UPBIT_SECRET_KEY");

  if (!order_test_mode_ && (options.access_key.empty() || options.secret_key.empty())) {
    throw std::runtime_error("live mode requires UPBIT_ACCESS_KEY and UPBIT_SECRET_KEY");
  }

  live_allowed_markets_ = ParseCsvUpperSet(EnvOrDefault("AUTOBOT_LIVE_ALLOWED_MARKETS", ""));
  live_min_notional_krw_ = ParsePositiveDoubleEnv("AUTOBOT_LIVE_MIN_NOTIONAL_KRW", 0.0);
  private_ws_enabled_ =
      !order_test_mode_ && ParseBoolEnv("AUTOBOT_EXECUTOR_PRIVATE_WS_ENABLED", true);
  private_ws_url_ =
      EnvOrDefault("AUTOBOT_UPBIT_PRIVATE_WS_URL", "wss://api.upbit.com/websocket/v1/private");
  private_ws_order_codes_ =
      ParseCsvUpperVector(EnvOrDefault("AUTOBOT_EXECUTOR_PRIVATE_WS_ORDER_CODES", ""));
  if (private_ws_order_codes_.empty() && !live_allowed_markets_.empty()) {
    private_ws_order_codes_.reserve(live_allowed_markets_.size());
    for (const auto& market : live_allowed_markets_) {
      private_ws_order_codes_.push_back(market);
    }
    std::sort(private_ws_order_codes_.begin(), private_ws_order_codes_.end());
  }
  if (!options.access_key.empty() && !options.secret_key.empty()) {
    private_ws_signer_.emplace(options.access_key, options.secret_key);
  }

  if (http_client_override != nullptr) {
    http_client_ = std::move(http_client_override);
  } else {
    http_client_ = std::make_unique<upbit::UpbitHttpClient>(options);
  }
  private_client_ = std::make_unique<upbit::UpbitPrivateClient>(http_client_.get());
  state_store_.Load();
}

bool UpbitRestClient::IsOrderTestMode() const {
  return order_test_mode_;
}

const std::string& UpbitRestClient::ModeName() const {
  return mode_name_;
}

bool UpbitRestClient::PrivateWsEnabled() const {
  return private_ws_enabled_ && !order_test_mode_ && private_ws_signer_.has_value();
}

const std::string& UpbitRestClient::PrivateWsUrl() const {
  return private_ws_url_;
}

std::vector<std::string> UpbitRestClient::PrivateWsOrderCodes() const {
  return private_ws_order_codes_;
}

std::string UpbitRestClient::PrivateWsAuthorizationHeader() const {
  if (!private_ws_signer_.has_value()) {
    return "";
  }
  return private_ws_signer_->BuildAuthorizationHeader("");
}

UpbitSubmitResult UpbitRestClient::SubmitLimitOrder(const UpbitSubmitRequest& request) {
  UpbitSubmitResult result;
  result.identifier = request.identifier;

  if (request.identifier.empty()) {
    result.reason = "identifier is required";
    return result;
  }
  if (request.market.empty()) {
    result.reason = "market is required";
    return result;
  }
  if (request.side != "bid" && request.side != "ask") {
    result.reason = "side must be bid or ask";
    return result;
  }
  if (request.price <= 0.0 || request.volume <= 0.0) {
    result.reason = "price and volume must be positive";
    return result;
  }

  const std::string market = ToUpper(request.market);
  if (!order_test_mode_) {
    if (!IsLiveMarketAllowed(market)) {
      result.reason = "market is not allowed in live mode";
      return result;
    }
    if (live_min_notional_krw_ > 0.0 && StartsWith(market, "KRW-")) {
      const double notional = request.price * request.volume;
      if (notional < live_min_notional_krw_) {
        result.reason = "live order notional below AUTOBOT_LIVE_MIN_NOTIONAL_KRW";
        return result;
      }
    }
  }

  std::string tif_validation_error;
  const std::optional<std::string> tif =
      upbit::NormalizeTimeInForce("limit", request.tif, &tif_validation_error);
  if (!tif_validation_error.empty()) {
    result.reason = tif_validation_error;
    return result;
  }

  const std::string identifier = request.identifier;
  if (!order_test_mode_) {
    const auto existing = state_store_.Find(identifier);
    if (existing.has_value()) {
      const UpbitOrderResult recovered = GetOrder("", identifier);
      if (recovered.ok && recovered.found && !recovered.upbit_uuid.empty()) {
        state::IdentifierStateRecord confirmed = *existing;
        confirmed.status = "CONFIRMED";
        confirmed.upbit_uuid = recovered.upbit_uuid;
        confirmed.updated_at_ms = NowMs();
        state_store_.Upsert(confirmed);

        result.accepted = true;
        result.reason = "accepted_existing_identifier";
        result.upbit_uuid = recovered.upbit_uuid;
        result.identifier = identifier;
        result.state = recovered.state.empty() ? "wait" : recovered.state;
        result.remaining_req_group = recovered.remaining_req_group;
        result.remaining_req_sec = recovered.remaining_req_sec;
        return result;
      }
      result.accepted = false;
      result.reason = "identifier_reuse_forbidden_new_identifier_required";
      result.retriable = false;
      return result;
    }
  }

  const std::string price_str = upbit::FormatPriceString(request.price, 0.0, 16);
  const std::string volume_str = upbit::FormatVolumeString(request.volume, 16);

  upbit::OrderCreateRequest create;
  create.market = market;
  create.side = ToLower(request.side);
  create.ord_type = "limit";
  create.price = price_str;
  create.volume = volume_str;
  create.time_in_force = tif;
  create.identifier = identifier;

  state::IdentifierStateRecord record = BuildStateRecord(identifier, request.intent_id, mode_name_, "NEW");
  state_store_.Upsert(record);
  record.status = "POST_SENT";
  record.updated_at_ms = NowMs();
  state_store_.Upsert(record);

  const upbit::HttpResponse response = private_client_->CreateOrder(create, order_test_mode_);
  result.http_status = response.status_code;
  result.error_name = response.error_name;
  result.breaker_state = response.breaker_state;
  ApplyRemainingReqToSubmitResult(&result, response);
  ApplyHttpMetaToRecord(&record, response);

  if (order_test_mode_) {
    if (!response.ok) {
      record.status = "FAILED";
      state_store_.Upsert(record);
      result.accepted = false;
      result.reason = "order_test_failed: " + BuildErrorReason(response, "order_test_failed");
      result.retriable = false;
      return result;
    }
    result.accepted = true;
    result.reason = "accepted_in_order_test_mode";
    result.identifier = identifier;
    result.upbit_uuid.clear();
    result.state = "wait";
    record.status = "CONFIRMED";
    record.upbit_uuid.clear();
    state_store_.Upsert(record);
    return result;
  }

  const upbit::RecoveryDecision decision = upbit::DecideCreateOrderRecovery(response);
  if (decision.action == upbit::RecoveryAction::kSuccess) {
    result.accepted = true;
    result.reason = "accepted";
    result.upbit_uuid = ParseJsonString(response.json_body, "uuid");
    result.identifier = ParseJsonString(response.json_body, "identifier");
    if (result.identifier.empty()) {
      result.identifier = identifier;
    }
    result.state = ParseJsonString(response.json_body, "state");
    if (result.state.empty()) {
      result.state = "wait";
    }

    record.status = "CONFIRMED";
    record.upbit_uuid = result.upbit_uuid;
    state_store_.Upsert(record);
    return result;
  }

  if (decision.action == upbit::RecoveryAction::kRecoverByGetIdentifier) {
    const UpbitOrderResult recovered = GetOrder("", identifier);
    if (recovered.ok && recovered.found && !recovered.upbit_uuid.empty()) {
      result.accepted = true;
      result.recovered_by_get = true;
      result.reason = "accepted_recovered_by_identifier_lookup";
      result.identifier = identifier;
      result.upbit_uuid = recovered.upbit_uuid;
      result.state = recovered.state.empty() ? "wait" : recovered.state;
      result.remaining_req_group = recovered.remaining_req_group;
      result.remaining_req_sec = recovered.remaining_req_sec;

      record.status = "CONFIRMED";
      record.upbit_uuid = recovered.upbit_uuid;
      state_store_.Upsert(record);
      return result;
    }
    if (recovered.ok && !recovered.found) {
      result.accepted = false;
      result.retriable = false;
      result.reason = "submit_unknown_lookup_not_found_new_identifier_required";
      record.status = "FAILED";
      state_store_.Upsert(record);
      return result;
    }

    result.accepted = false;
    result.retriable = false;
    result.reason = "submit_unknown_lookup_failed_operator_intervention_required";
    result.operator_intervention_required = true;
    record.status = "UNKNOWN";
    record.last_error_name = recovered.reason;
    state_store_.Upsert(record);
    return result;
  }

  result.accepted = false;
  result.reason = BuildErrorReason(response, decision.reason);
  result.retriable = false;
  result.operator_intervention_required = decision.operator_intervention_required;
  record.status = "FAILED";
  state_store_.Upsert(record);
  return result;
}

UpbitCancelResult UpbitRestClient::CancelOrder(const UpbitCancelRequest& request) {
  UpbitCancelResult result;

  std::string resolved_uuid = request.upbit_uuid;
  std::string resolved_identifier = request.identifier;
  if (resolved_uuid.empty() && !resolved_identifier.empty()) {
    const auto existing = state_store_.Find(resolved_identifier);
    if (existing.has_value() && !existing->upbit_uuid.empty()) {
      resolved_uuid = existing->upbit_uuid;
    }
  }

  if (resolved_uuid.empty() && resolved_identifier.empty()) {
    result.accepted = false;
    result.reason = "upbit_uuid or identifier is required";
    return result;
  }

  if (order_test_mode_) {
    result.accepted = true;
    result.reason = "cancelled_local_ack_order_test_mode";
    result.upbit_uuid = resolved_uuid;
    result.identifier = resolved_identifier;
    result.state = "cancel";

    if (!resolved_identifier.empty()) {
      auto record = state_store_.Find(resolved_identifier).value_or(
          BuildStateRecord(resolved_identifier, "", mode_name_, "CANCELED"));
      record.status = "CANCELED";
      record.updated_at_ms = NowMs();
      state_store_.Upsert(record);
    }
    return result;
  }

  const upbit::HttpResponse response = private_client_->CancelOrder(resolved_uuid, resolved_identifier);
  result.http_status = response.status_code;
  result.error_name = response.error_name;
  result.breaker_state = response.breaker_state;
  ApplyRemainingReqToCancelResult(&result, response);

  if (!response.ok) {
    result.accepted = false;
    result.reason = BuildErrorReason(response, "cancel_failed");
    result.upbit_uuid = resolved_uuid;
    result.identifier = resolved_identifier;
    result.retriable = response.retriable;
    result.state = "cancel_reject";
    return result;
  }

  result.accepted = true;
  result.reason = "cancelled";
  result.upbit_uuid = ParseJsonString(response.json_body, "uuid");
  if (result.upbit_uuid.empty()) {
    result.upbit_uuid = resolved_uuid;
  }
  result.identifier = ParseJsonString(response.json_body, "identifier");
  if (result.identifier.empty()) {
    result.identifier = resolved_identifier;
  }
  result.state = ParseJsonString(response.json_body, "state");
  if (result.state.empty()) {
    result.state = "cancel";
  }

  if (!result.identifier.empty()) {
    auto record = state_store_.Find(result.identifier).value_or(
        BuildStateRecord(result.identifier, "", mode_name_, "CANCELED"));
    ApplyHttpMetaToRecord(&record, response);
    record.status = "CANCELED";
    if (!result.upbit_uuid.empty()) {
      record.upbit_uuid = result.upbit_uuid;
    }
    state_store_.Upsert(record);
  }

  return result;
}

UpbitReplaceResult UpbitRestClient::ReplaceOrder(const UpbitReplaceRequest& request) {
  UpbitReplaceResult result;

  const std::string prev_uuid = Trim(request.prev_order_uuid);
  const std::string prev_identifier = Trim(request.prev_order_identifier);
  const std::string new_identifier = Trim(request.new_identifier);
  const std::string new_price = Trim(request.new_price_str);
  const std::string new_volume = Trim(request.new_volume_str);
  result.new_identifier = new_identifier;

  if (prev_uuid.empty() && prev_identifier.empty()) {
    result.reason = "prev_order_uuid or prev_order_identifier is required";
    return result;
  }
  if (new_identifier.empty()) {
    result.reason = "new_identifier is required";
    return result;
  }
  if (new_price.empty() || !IsPositiveNumberString(new_price)) {
    result.reason = "new_price_str must be a positive number string";
    return result;
  }
  if (new_volume.empty()) {
    result.reason = "new_volume_str is required";
    return result;
  }
  if (ToLower(new_volume) != "remain_only" && !IsPositiveNumberString(new_volume)) {
    result.reason = "new_volume_str must be a positive number string or remain_only";
    return result;
  }

  std::string new_tif_validation_error;
  const std::optional<std::string> new_tif =
      upbit::NormalizeTimeInForce("limit", request.new_time_in_force, &new_tif_validation_error);
  if (!new_tif_validation_error.empty()) {
    result.reason = new_tif_validation_error;
    return result;
  }

  if (order_test_mode_) {
    result.reason = "replace_not_supported_in_order_test_mode";
    return result;
  }

  std::optional<state::IdentifierStateRecord> prev_record;
  if (!prev_identifier.empty()) {
    prev_record = state_store_.Find(prev_identifier);
  }
  if (!prev_record.has_value() && !prev_uuid.empty()) {
    prev_record = state_store_.FindByUpbitUuid(prev_uuid);
  }

  std::string resolved_prev_identifier = prev_identifier;
  if (resolved_prev_identifier.empty() && prev_record.has_value()) {
    resolved_prev_identifier = prev_record->identifier;
  }

  std::string resolved_prev_uuid = prev_uuid;
  if (resolved_prev_uuid.empty() && prev_record.has_value()) {
    resolved_prev_uuid = prev_record->upbit_uuid;
  }

  std::string root_identifier = resolved_prev_identifier;
  std::string root_upbit_uuid = resolved_prev_uuid;
  int replace_attempt = 1;
  if (prev_record.has_value()) {
    if (!prev_record->root_identifier.empty()) {
      root_identifier = prev_record->root_identifier;
    } else if (root_identifier.empty()) {
      root_identifier = prev_record->identifier;
    }
    if (!prev_record->root_upbit_uuid.empty()) {
      root_upbit_uuid = prev_record->root_upbit_uuid;
    } else if (root_upbit_uuid.empty()) {
      root_upbit_uuid = prev_record->upbit_uuid;
    }
    replace_attempt = std::max(prev_record->replace_attempt + 1, 1);
  }

  if (const auto existing = state_store_.Find(new_identifier); existing.has_value()) {
    result.reason = "identifier_reuse_forbidden_new_identifier_required";
    return result;
  }

  upbit::CancelAndNewRequest replace_request;
  replace_request.prev_order_uuid = prev_uuid;
  replace_request.prev_order_identifier = prev_uuid.empty() ? prev_identifier : "";
  replace_request.new_identifier = new_identifier;
  replace_request.new_price = new_price;
  replace_request.new_volume = new_volume;
  replace_request.new_time_in_force = new_tif;

  const std::int64_t replace_started_ts_ms = NowMs();
  state::IdentifierStateRecord new_record =
      BuildStateRecord(new_identifier, request.intent_id, mode_name_, "REPLACE_POST_SENT");
  new_record.prev_identifier = resolved_prev_identifier;
  new_record.prev_upbit_uuid = resolved_prev_uuid;
  new_record.root_identifier = root_identifier;
  new_record.root_upbit_uuid = root_upbit_uuid;
  new_record.chain_status = "REPLACE_PENDING";
  new_record.replace_attempt = replace_attempt;
  new_record.last_replace_ts_ms = replace_started_ts_ms;
  state_store_.Upsert(new_record);

  const upbit::HttpResponse response = private_client_->CancelAndNewOrder(replace_request);
  result.http_status = response.status_code;
  result.error_name = response.error_name;
  result.breaker_state = response.breaker_state;
  if (response.remaining_req.valid) {
    result.remaining_req_group = response.remaining_req.group;
    result.remaining_req_sec = response.remaining_req.sec;
  }
  ApplyHttpMetaToRecord(&new_record, response);

  if (!response.ok) {
    result.accepted = false;
    result.reason = BuildErrorReason(response, "replace_failed");
    result.retriable = response.retriable;
    new_record.status = "FAILED";
    new_record.chain_status = "REPLACE_FAILED";
    new_record.last_replace_ts_ms = NowMs();
    state_store_.Upsert(new_record);
    return result;
  }

  auto pick_string = [&](std::initializer_list<const char*> keys) -> std::string {
    for (const char* key : keys) {
      if (key == nullptr) {
        continue;
      }
      const std::string value = ParseJsonString(response.json_body, key);
      if (!value.empty()) {
        return value;
      }
    }
    return "";
  };

  result.accepted = true;
  result.reason = "replaced";
  result.cancelled_order_uuid = pick_string(
      {"cancelled_order_uuid", "canceled_order_uuid", "prev_order_uuid", "cancel_uuid"});
  if (result.cancelled_order_uuid.empty()) {
    result.cancelled_order_uuid = resolved_prev_uuid;
  }

  result.new_order_uuid = pick_string({"new_order_uuid", "new_uuid", "order_uuid"});
  if (result.new_order_uuid.empty()) {
    const std::string direct_uuid = ParseJsonString(response.json_body, "uuid");
    if (!direct_uuid.empty() && direct_uuid != result.cancelled_order_uuid) {
      result.new_order_uuid = direct_uuid;
    }
  }

  if (result.new_order_uuid.empty()) {
    const UpbitOrderResult recovered = GetOrder("", new_identifier);
    if (recovered.ok && recovered.found && !recovered.upbit_uuid.empty()) {
      result.new_order_uuid = recovered.upbit_uuid;
      result.reason = "replaced_lookup_confirmed";
    } else if (recovered.ok && !recovered.found) {
      result.reason = "prev_order_filled_before_cancel_new_order_not_created";
    } else {
      result.reason = "replace_accepted_new_order_unconfirmed";
    }
  }

  new_record.status = result.new_order_uuid.empty() ? "CONFIRMED_NO_NEW_ORDER" : "CONFIRMED";
  new_record.upbit_uuid = result.new_order_uuid;
  if (new_record.root_identifier.empty()) {
    new_record.root_identifier = resolved_prev_identifier.empty() ? new_identifier : resolved_prev_identifier;
  }
  if (new_record.root_upbit_uuid.empty()) {
    new_record.root_upbit_uuid = resolved_prev_uuid;
  }
  new_record.chain_status =
      result.new_order_uuid.empty() ? "REPLACE_CONFIRMED_NO_NEW_ORDER" : "REPLACE_CONFIRMED";
  new_record.last_replace_ts_ms = NowMs();
  state_store_.Upsert(new_record);

  if (!resolved_prev_identifier.empty()) {
    auto prev_state = state_store_.Find(resolved_prev_identifier).value_or(
        BuildStateRecord(resolved_prev_identifier, "", mode_name_, "REPLACED"));
    prev_state.status = "REPLACED";
    prev_state.chain_status = "REPLACED_BY_SUCCESSOR";
    if (prev_state.root_identifier.empty()) {
      prev_state.root_identifier =
          root_identifier.empty() ? resolved_prev_identifier : root_identifier;
    }
    if (prev_state.root_upbit_uuid.empty() && !root_upbit_uuid.empty()) {
      prev_state.root_upbit_uuid = root_upbit_uuid;
    }
    prev_state.replace_attempt = std::max(prev_state.replace_attempt, replace_attempt - 1);
    prev_state.last_replace_ts_ms = NowMs();
    if (!result.cancelled_order_uuid.empty()) {
      prev_state.upbit_uuid = result.cancelled_order_uuid;
    }
    prev_state.updated_at_ms = NowMs();
    state_store_.Upsert(prev_state);
  }

  return result;
}

UpbitOrderResult UpbitRestClient::GetOrder(const std::string& upbit_uuid, const std::string& identifier) {
  UpbitOrderResult result;
  if (upbit_uuid.empty() && identifier.empty()) {
    result.reason = "upbit_uuid or identifier is required";
    return result;
  }
  if (order_test_mode_) {
    result.reason = "order_test_mode_no_remote_state";
    return result;
  }

  const upbit::HttpResponse response = private_client_->GetOrder(upbit_uuid, identifier);
  result.http_status = response.status_code;
  result.error_name = response.error_name;
  result.breaker_state = response.breaker_state;
  if (response.remaining_req.valid) {
    result.remaining_req_group = response.remaining_req.group;
    result.remaining_req_sec = response.remaining_req.sec;
  }

  if (!response.ok) {
    if (response.status_code == 404) {
      result.ok = true;
      result.found = false;
      result.reason = "not_found";
      return result;
    }
    result.ok = false;
    result.found = false;
    result.reason = BuildErrorReason(response, "get_order_failed");
    result.retriable = response.retriable;
    return result;
  }

  result.ok = true;
  result.found = true;
  result.reason = "ok";
  result.upbit_uuid = ParseJsonString(response.json_body, "uuid");
  result.identifier = ParseJsonString(response.json_body, "identifier");
  result.market = ParseJsonString(response.json_body, "market");
  result.side = ParseJsonString(response.json_body, "side");
  result.ord_type = ParseJsonString(response.json_body, "ord_type");
  result.state = ParseJsonString(response.json_body, "state");
  result.price_str = ParseJsonString(response.json_body, "price");
  result.volume_str = ParseJsonString(response.json_body, "volume");
  result.executed_volume_str = ParseJsonString(response.json_body, "executed_volume");
  result.remaining_volume_str = ParseJsonString(response.json_body, "remaining_volume");
  result.avg_price_str = ParseJsonString(response.json_body, "avg_price");

  if (!result.identifier.empty()) {
    auto record = state_store_.Find(result.identifier).value_or(
        BuildStateRecord(result.identifier, "", mode_name_, "CONFIRMED"));
    ApplyHttpMetaToRecord(&record, response);
    record.status = "CONFIRMED";
    if (!result.upbit_uuid.empty()) {
      record.upbit_uuid = result.upbit_uuid;
    }
    state_store_.Upsert(record);
  }
  return result;
}

UpbitAccountsSnapshotResult UpbitRestClient::GetAccountsSnapshot() {
  UpbitAccountsSnapshotResult result;
  if (order_test_mode_) {
    result.reason = "order_test_mode_no_remote_accounts";
    return result;
  }

  const upbit::HttpResponse response = private_client_->Accounts();
  result.http_status = response.status_code;
  result.error_name = response.error_name;
  result.breaker_state = response.breaker_state;
  if (response.remaining_req.valid) {
    result.remaining_req_group = response.remaining_req.group;
    result.remaining_req_sec = response.remaining_req.sec;
  }

  if (!response.ok) {
    result.ok = false;
    result.reason = BuildErrorReason(response, "accounts_snapshot_failed");
    return result;
  }

  result.ok = true;
  result.reason = "ok";
  if (!response.json_body.is_array()) {
    return result;
  }
  for (const auto& item : response.json_body) {
    if (!item.is_object()) {
      continue;
    }
    UpbitAccountBalance account;
    account.currency = ToUpper(ParseJsonString(item, "currency"));
    if (account.currency.empty()) {
      continue;
    }
    account.balance_str = ParseJsonString(item, "balance");
    account.locked_str = ParseJsonString(item, "locked");
    account.avg_buy_price_str = ParseJsonString(item, "avg_buy_price");
    if (account.balance_str.empty()) {
      account.balance_str = "0";
    }
    if (account.locked_str.empty()) {
      account.locked_str = "0";
    }
    result.accounts.push_back(std::move(account));
  }
  return result;
}

std::string UpbitRestClient::ParseJsonString(const nlohmann::json& payload, const char* key) {
  if (!payload.is_object() || key == nullptr) {
    return "";
  }
  const auto found = payload.find(key);
  if (found == payload.end() || found->is_null()) {
    return "";
  }
  if (found->is_string()) {
    return found->get<std::string>();
  }
  if (found->is_number_integer()) {
    return std::to_string(found->get<long long>());
  }
  if (found->is_number_unsigned()) {
    return std::to_string(found->get<unsigned long long>());
  }
  if (found->is_number_float()) {
    return upbit::FormatNumberString(found->get<double>(), 16);
  }
  return "";
}

std::string UpbitRestClient::ResolveStateFilePath() {
  return EnvOrDefault("AUTOBOT_EXECUTOR_STATE_PATH", "data/state/executor_state.json");
}

bool UpbitRestClient::IsLiveMarketAllowed(const std::string& market) const {
  if (order_test_mode_ || live_allowed_markets_.empty()) {
    return true;
  }
  return live_allowed_markets_.count(ToUpper(market)) > 0;
}

}  // namespace autobot::executor
