#include "tests/fault_injection_transport.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <thread>

namespace autobot::executor::tests {

namespace {

double ClampProbability(double value) {
  if (value < 0.0) {
    return 0.0;
  }
  if (value > 1.0) {
    return 1.0;
  }
  return value;
}

double NextUnit(unsigned int* state) {
  if (state == nullptr) {
    return 0.0;
  }
  *state = (*state * 1664525U) + 1013904223U;
  return static_cast<double>(*state & 0x00FFFFFFU) / static_cast<double>(0x01000000U);
}

}  // namespace

FaultInjectionTransport::FaultInjectionTransport() {
  default_action_.kind = FaultInjectionAction::Kind::kHttp;
  default_action_.status_code = 200;
  default_action_.body = "{}";
}

FaultInjectionTransport::FaultInjectionTransport(std::vector<FaultInjectionRule> rules)
    : FaultInjectionTransport() {
  for (auto& rule : rules) {
    AddRule(std::move(rule));
  }
}

void FaultInjectionTransport::AddRule(FaultInjectionRule rule) {
  rule.method = NormalizeMethod(std::move(rule.method));
  rule.endpoint = NormalizeEndpoint(std::move(rule.endpoint));
  rule.probability = ClampProbability(rule.probability);

  std::lock_guard<std::mutex> lock(mutex_);
  rules_.push_back(std::move(rule));
}

void FaultInjectionTransport::SetDefaultHttp(
    int status_code,
    std::string body,
    std::unordered_map<std::string, std::string> headers) {
  std::lock_guard<std::mutex> lock(mutex_);
  default_action_.kind = FaultInjectionAction::Kind::kHttp;
  default_action_.status_code = status_code;
  default_action_.body = std::move(body);
  default_action_.headers = LowercaseHeaders(headers);
}

int FaultInjectionTransport::CallCount(const std::string& method, const std::string& endpoint) const {
  const std::string key = CallKey(NormalizeMethod(method), NormalizeEndpoint(endpoint));
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = call_counts_.find(key);
  if (found == call_counts_.end()) {
    return 0;
  }
  return found->second;
}

upbit::UpbitHttpClient::RawResponse FaultInjectionTransport::PerformRequest(
    const std::string& method_upper,
    const std::string& endpoint,
    const std::string& encoded_query,
    const std::unordered_map<std::string, std::string>& headers,
    const std::string& body_json) {
  (void)encoded_query;
  (void)headers;
  (void)body_json;

  const std::string method = NormalizeMethod(method_upper);
  const std::string path = NormalizeEndpoint(endpoint);
  const std::string key = CallKey(method, path);

  FaultInjectionAction chosen;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    int& call_index = call_counts_[key];
    call_index += 1;
    chosen = default_action_;

    for (const auto& rule : rules_) {
      if (rule.method != method || rule.endpoint != path) {
        continue;
      }
      if (rule.nth_call > 0 && rule.nth_call != call_index) {
        continue;
      }
      if (rule.probability < 1.0 && NextUnit(&rng_state_) > rule.probability) {
        continue;
      }
      chosen = rule.action;
      break;
    }
  }

  if (chosen.delay_ms > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(chosen.delay_ms));
  }

  upbit::UpbitHttpClient::RawResponse raw;
  if (chosen.kind == FaultInjectionAction::Kind::kNetworkError) {
    raw.network_ok = false;
    raw.network_error = chosen.network_error.empty() ? "injected_network_error" : chosen.network_error;
    return raw;
  }

  raw.network_ok = true;
  raw.status_code = chosen.status_code;
  raw.body = chosen.body;
  raw.headers = LowercaseHeaders(chosen.headers);
  return raw;
}

std::string FaultInjectionTransport::NormalizeMethod(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  if (value.empty()) {
    return "GET";
  }
  return value;
}

std::string FaultInjectionTransport::NormalizeEndpoint(std::string value) {
  if (value.empty()) {
    return "/";
  }
  if (value.front() != '/') {
    value.insert(value.begin(), '/');
  }
  return value;
}

std::string FaultInjectionTransport::CallKey(const std::string& method, const std::string& endpoint) {
  return method + " " + endpoint;
}

std::unordered_map<std::string, std::string> FaultInjectionTransport::LowercaseHeaders(
    const std::unordered_map<std::string, std::string>& headers) {
  std::unordered_map<std::string, std::string> out;
  out.reserve(headers.size());
  for (const auto& [key, value] : headers) {
    std::string lowered = key;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
      return static_cast<char>(std::tolower(ch));
    });
    out[lowered] = value;
  }
  return out;
}

}  // namespace autobot::executor::tests
