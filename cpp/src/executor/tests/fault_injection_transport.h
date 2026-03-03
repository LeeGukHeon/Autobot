#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "upbit/http_client.h"

namespace autobot::executor::tests {

struct FaultInjectionAction {
  enum class Kind {
    kHttp = 0,
    kNetworkError = 1,
  };

  Kind kind = Kind::kHttp;
  int status_code = 200;
  std::string body = "{}";
  std::unordered_map<std::string, std::string> headers;
  std::string network_error = "injected_network_error";
  int delay_ms = 0;
};

struct FaultInjectionRule {
  std::string method = "GET";
  std::string endpoint;
  int nth_call = -1;      // -1 means all calls.
  double probability = 1;  // [0,1]
  FaultInjectionAction action;
};

class FaultInjectionTransport final : public upbit::UpbitHttpClient::ITransport {
 public:
  FaultInjectionTransport();
  explicit FaultInjectionTransport(std::vector<FaultInjectionRule> rules);

  void AddRule(FaultInjectionRule rule);
  void SetDefaultHttp(
      int status_code,
      std::string body,
      std::unordered_map<std::string, std::string> headers = {});

  int CallCount(const std::string& method, const std::string& endpoint) const;

  upbit::UpbitHttpClient::RawResponse PerformRequest(
      const std::string& method_upper,
      const std::string& endpoint,
      const std::string& encoded_query,
      const std::unordered_map<std::string, std::string>& headers,
      const std::string& body_json) override;

 private:
  static std::string NormalizeMethod(std::string value);
  static std::string NormalizeEndpoint(std::string value);
  static std::string CallKey(const std::string& method, const std::string& endpoint);
  static std::unordered_map<std::string, std::string> LowercaseHeaders(
      const std::unordered_map<std::string, std::string>& headers);

  mutable std::mutex mutex_;
  std::vector<FaultInjectionRule> rules_;
  std::unordered_map<std::string, int> call_counts_;
  FaultInjectionAction default_action_;
  unsigned int rng_state_ = 0xC6C6C6C6U;
};

}  // namespace autobot::executor::tests
