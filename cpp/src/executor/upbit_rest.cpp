#include "upbit_rest.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include <nlohmann/json.hpp>

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

}  // namespace

UpbitRestClient::UpbitRestClient(bool order_test_mode) : order_test_mode_(order_test_mode) {
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

  http_client_ = std::make_unique<upbit::UpbitHttpClient>(options);
  private_client_ = std::make_unique<upbit::UpbitPrivateClient>(http_client_.get());
  state_file_path_ = ResolveStateFilePath();
  LoadState();
}

bool UpbitRestClient::IsOrderTestMode() const {
  return order_test_mode_;
}

UpbitSubmitResult UpbitRestClient::SubmitLimitOrder(const UpbitSubmitRequest& request) {
  if (request.identifier.empty()) {
    UpbitSubmitResult rejected;
    rejected.accepted = false;
    rejected.reason = "identifier is required";
    return rejected;
  }
  if (request.market.empty()) {
    UpbitSubmitResult rejected;
    rejected.accepted = false;
    rejected.reason = "market is required";
    return rejected;
  }
  if (request.side != "bid" && request.side != "ask") {
    UpbitSubmitResult rejected;
    rejected.accepted = false;
    rejected.reason = "side must be bid or ask";
    return rejected;
  }
  if (request.price <= 0.0 || request.volume <= 0.0) {
    UpbitSubmitResult rejected;
    rejected.accepted = false;
    rejected.reason = "price and volume must be positive";
    return rejected;
  }

  upbit::OrderCreateRequest create;
  create.market = ToUpper(request.market);
  create.side = request.side;
  create.ord_type = "limit";
  create.price = FormatNumber(request.price);
  create.volume = FormatNumber(request.volume);
  create.time_in_force = request.tif;
  create.identifier = request.identifier;

  if (order_test_mode_) {
    const upbit::HttpResponse response = private_client_->CreateOrder(create, true);
    if (!response.ok) {
      UpbitSubmitResult rejected;
      rejected.accepted = false;
      rejected.reason = "order_test_failed: " + BuildErrorReason(response, "order_test_failed");
      rejected.retriable = response.retriable;
      rejected.remaining_req_group = response.remaining_req.valid ? response.remaining_req.group : "default";
      rejected.remaining_req_sec = response.remaining_req.valid ? response.remaining_req.sec : -1;
      return rejected;
    }

    UpbitSubmitResult result;
    result.accepted = true;
    result.reason = "accepted_in_order_test_mode";
    result.upbit_uuid = ParseJsonString(response.json_body, "uuid");
    if (result.upbit_uuid.empty()) {
      result.upbit_uuid = BuildMockUuid(request.identifier);
    }
    result.identifier = request.identifier;
    result.state = "wait";
    result.remaining_req_group = response.remaining_req.valid ? response.remaining_req.group : "default";
    result.remaining_req_sec = response.remaining_req.valid ? response.remaining_req.sec : -1;
    return result;
  }

  const std::string cached_uuid = ResolveMappedUuid(request.identifier);
  if (!cached_uuid.empty()) {
    UpbitSubmitResult result;
    result.accepted = true;
    result.reason = "accepted_existing_identifier";
    result.upbit_uuid = cached_uuid;
    result.identifier = request.identifier;
    result.state = "wait";
    return result;
  }

  const UpbitOrderResult existing = GetOrder("", request.identifier);
  if (existing.ok && existing.found && !existing.upbit_uuid.empty()) {
    UpsertIdentifierMapping(request.identifier, existing.upbit_uuid);
    UpbitSubmitResult result;
    result.accepted = true;
    result.reason = "accepted_existing_identifier";
    result.upbit_uuid = existing.upbit_uuid;
    result.identifier = request.identifier;
    result.state = existing.state.empty() ? "wait" : existing.state;
    result.remaining_req_group = existing.remaining_req_group;
    result.remaining_req_sec = existing.remaining_req_sec;
    return result;
  }
  if (!existing.ok && existing.reason != "not_found") {
    UpbitSubmitResult rejected;
    rejected.accepted = false;
    rejected.reason = "identifier_lookup_failed: " + existing.reason;
    rejected.retriable = existing.retriable;
    rejected.remaining_req_group = existing.remaining_req_group;
    rejected.remaining_req_sec = existing.remaining_req_sec;
    return rejected;
  }

  const upbit::HttpResponse response = private_client_->CreateOrder(create, false);
  if (!response.ok) {
    UpbitSubmitResult rejected;
    rejected.accepted = false;
    rejected.reason = BuildErrorReason(response, "submit_failed");
    rejected.retriable = response.retriable;
    rejected.remaining_req_group = response.remaining_req.valid ? response.remaining_req.group : "default";
    rejected.remaining_req_sec = response.remaining_req.valid ? response.remaining_req.sec : -1;
    return rejected;
  }

  UpbitSubmitResult result;
  result.accepted = true;
  result.reason = "accepted";
  result.upbit_uuid = ParseJsonString(response.json_body, "uuid");
  result.identifier = ParseJsonString(response.json_body, "identifier");
  if (result.identifier.empty()) {
    result.identifier = request.identifier;
  }
  result.state = ParseJsonString(response.json_body, "state");
  if (result.state.empty()) {
    result.state = "wait";
  }
  result.remaining_req_group = response.remaining_req.valid ? response.remaining_req.group : "default";
  result.remaining_req_sec = response.remaining_req.valid ? response.remaining_req.sec : -1;
  if (!result.identifier.empty() && !result.upbit_uuid.empty()) {
    UpsertIdentifierMapping(result.identifier, result.upbit_uuid);
  }
  return result;
}

UpbitCancelResult UpbitRestClient::CancelOrder(const UpbitCancelRequest& request) {
  if (request.upbit_uuid.empty() && request.identifier.empty()) {
    UpbitCancelResult rejected;
    rejected.accepted = false;
    rejected.reason = "upbit_uuid or identifier is required";
    return rejected;
  }

  const std::string resolved_identifier = request.identifier;
  std::string resolved_uuid = request.upbit_uuid;
  if (resolved_uuid.empty() && !request.identifier.empty()) {
    resolved_uuid = ResolveMappedUuid(request.identifier);
  }

  if (order_test_mode_) {
    UpbitCancelResult result;
    result.accepted = true;
    result.reason = "cancelled_in_order_test_mode";
    result.upbit_uuid = resolved_uuid.empty() ? BuildMockUuid(request.identifier) : resolved_uuid;
    result.identifier = resolved_identifier;
    result.state = "cancel";
    return result;
  }

  const upbit::HttpResponse response = private_client_->CancelOrder(resolved_uuid, request.identifier);
  if (!response.ok) {
    UpbitCancelResult rejected;
    rejected.accepted = false;
    rejected.reason = BuildErrorReason(response, "cancel_failed");
    rejected.upbit_uuid = resolved_uuid;
    rejected.identifier = resolved_identifier;
    rejected.retriable = response.retriable;
    rejected.remaining_req_group = response.remaining_req.valid ? response.remaining_req.group : "default";
    rejected.remaining_req_sec = response.remaining_req.valid ? response.remaining_req.sec : -1;
    rejected.state = "cancel_reject";
    return rejected;
  }

  UpbitCancelResult result;
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
  result.remaining_req_group = response.remaining_req.valid ? response.remaining_req.group : "default";
  result.remaining_req_sec = response.remaining_req.valid ? response.remaining_req.sec : -1;
  if (!result.identifier.empty() && !result.upbit_uuid.empty()) {
    UpsertIdentifierMapping(result.identifier, result.upbit_uuid);
  }
  return result;
}

UpbitOrderResult UpbitRestClient::GetOrder(const std::string& upbit_uuid, const std::string& identifier) {
  UpbitOrderResult result;
  if (upbit_uuid.empty() && identifier.empty()) {
    result.ok = false;
    result.reason = "upbit_uuid or identifier is required";
    return result;
  }
  if (order_test_mode_) {
    result.ok = false;
    result.reason = "order_test_mode_no_remote_state";
    return result;
  }

  const upbit::HttpResponse response = private_client_->GetOrder(upbit_uuid, identifier);
  result.remaining_req_group = response.remaining_req.valid ? response.remaining_req.group : "default";
  result.remaining_req_sec = response.remaining_req.valid ? response.remaining_req.sec : -1;

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
  result.price = ParseJsonNumber(response.json_body, "price");
  result.volume = ParseJsonNumber(response.json_body, "volume");
  result.executed_volume = ParseJsonNumber(response.json_body, "executed_volume");
  if (!result.identifier.empty() && !result.upbit_uuid.empty()) {
    UpsertIdentifierMapping(result.identifier, result.upbit_uuid);
  }
  return result;
}

double UpbitRestClient::ParseJsonNumber(const nlohmann::json& payload, const char* key) {
  if (!payload.is_object() || key == nullptr) {
    return 0.0;
  }
  const auto found = payload.find(key);
  if (found == payload.end() || found->is_null()) {
    return 0.0;
  }
  if (found->is_number_float() || found->is_number_integer() || found->is_number_unsigned()) {
    return found->get<double>();
  }
  if (found->is_string()) {
    try {
      return std::stod(found->get<std::string>());
    } catch (...) {
      return 0.0;
    }
  }
  return 0.0;
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
    std::ostringstream oss;
    oss << found->get<double>();
    return oss.str();
  }
  return "";
}

std::string UpbitRestClient::BuildMockUuid(const std::string& identifier) {
  if (identifier.empty()) {
    return "mock-order-unknown";
  }

  std::string normalized;
  normalized.reserve(identifier.size());
  for (const char ch : identifier) {
    if (std::isalnum(static_cast<unsigned char>(ch))) {
      normalized.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    } else {
      normalized.push_back('-');
    }
  }
  if (normalized.size() > 48) {
    normalized = normalized.substr(0, 48);
  }
  return "mock-order-" + normalized;
}

std::string UpbitRestClient::FormatNumber(double value) {
  std::ostringstream out;
  out << std::fixed << std::setprecision(16) << value;
  std::string text = out.str();
  while (!text.empty() && text.back() == '0') {
    text.pop_back();
  }
  if (!text.empty() && text.back() == '.') {
    text.pop_back();
  }
  if (text.empty()) {
    return "0";
  }
  return text;
}

void UpbitRestClient::LoadState() {
  std::lock_guard<std::mutex> lock(mutex_);
  identifier_to_uuid_.clear();
  if (state_file_path_.empty()) {
    return;
  }

  try {
    const std::filesystem::path path(state_file_path_);
    if (!std::filesystem::exists(path)) {
      return;
    }
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
      return;
    }
    nlohmann::json payload = nlohmann::json::parse(in, nullptr, false);
    if (!payload.is_object()) {
      return;
    }
    const auto found = payload.find("identifier_to_uuid");
    if (found == payload.end() || !found->is_object()) {
      return;
    }
    for (auto it = found->begin(); it != found->end(); ++it) {
      if (!it.value().is_string()) {
        continue;
      }
      const std::string identifier = Trim(it.key());
      const std::string uuid = Trim(it.value().get<std::string>());
      if (!identifier.empty() && !uuid.empty()) {
        identifier_to_uuid_[identifier] = uuid;
      }
    }
  } catch (...) {
    // Keep empty in-memory state if state file is unavailable/corrupt.
  }
}

void UpbitRestClient::SaveStateLocked() const {
  if (state_file_path_.empty()) {
    return;
  }
  try {
    const std::filesystem::path path(state_file_path_);
    const std::filesystem::path parent = path.parent_path();
    if (!parent.empty()) {
      std::filesystem::create_directories(parent);
    }

    nlohmann::json payload;
    payload["identifier_to_uuid"] = nlohmann::json::object();
    for (const auto& [identifier, uuid] : identifier_to_uuid_) {
      payload["identifier_to_uuid"][identifier] = uuid;
    }

    const std::filesystem::path tmp = path.string() + ".tmp";
    {
      std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
      out << payload.dump(2);
    }
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
    std::filesystem::rename(tmp, path, ignored);
    if (ignored) {
      std::filesystem::copy_file(tmp, path, std::filesystem::copy_options::overwrite_existing, ignored);
      std::filesystem::remove(tmp, ignored);
    }
  } catch (...) {
    // Ignore persistence errors to keep order flow alive.
  }
}

void UpbitRestClient::UpsertIdentifierMapping(const std::string& identifier, const std::string& upbit_uuid) {
  if (identifier.empty() || upbit_uuid.empty()) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  identifier_to_uuid_[identifier] = upbit_uuid;
  SaveStateLocked();
}

std::string UpbitRestClient::ResolveMappedUuid(const std::string& identifier) const {
  if (identifier.empty()) {
    return "";
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = identifier_to_uuid_.find(identifier);
  if (found == identifier_to_uuid_.end()) {
    return "";
  }
  return found->second;
}

std::string UpbitRestClient::ResolveStateFilePath() {
  return EnvOrDefault("AUTOBOT_EXECUTOR_STATE_PATH", "data/state/executor_state.json");
}

}  // namespace autobot::executor
