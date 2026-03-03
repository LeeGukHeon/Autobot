#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <nlohmann/json.hpp>

#include "upbit/auth_jwt.h"
#include "upbit/rate_limiter.h"
#include "upbit/remaining_req.h"

namespace autobot::executor::upbit {

class DefaultHttpTransport;

struct HttpClientOptions {
  std::string base_url = "https://api.upbit.com";
  int connect_timeout_ms = 3000;
  int read_timeout_ms = 10000;
  int write_timeout_ms = 10000;
  int max_attempts = 3;
  int base_backoff_ms = 200;
  int max_backoff_ms = 2000;
  bool rate_limit_enabled = true;
  int ban_cooldown_sec = 60;
  std::unordered_map<std::string, double> group_rates;
  std::string access_key;
  std::string secret_key;
};

struct HttpRequest {
  std::string method;
  std::string endpoint;
  std::string url_query;
  std::string auth_query;
  std::string body_json;
  std::unordered_map<std::string, std::string> headers;
  bool auth = false;
  bool allow_retry = true;
  std::string rate_limit_group = "default";
};

struct HttpResponse {
  bool ok = false;
  int status_code = 0;
  nlohmann::json json_body = nlohmann::json::object();
  std::string raw_body;
  std::string error_name;
  std::string error_message;
  std::string category = "network";
  bool retriable = false;
  bool banned = false;
  double cooldown_sec = 0.0;
  std::string breaker_state = "none";
  RemainingReqInfo remaining_req;
  std::string request_id;
};

class UpbitHttpClient {
 public:
  struct RawResponse {
    bool network_ok = false;
    int status_code = 0;
    std::string body;
    std::unordered_map<std::string, std::string> headers;
    std::string network_error;
  };

  class ITransport {
   public:
    virtual ~ITransport() = default;
    virtual RawResponse PerformRequest(
        const std::string& method_upper,
        const std::string& endpoint,
        const std::string& encoded_query,
        const std::unordered_map<std::string, std::string>& headers,
        const std::string& body_json) = 0;
  };

  explicit UpbitHttpClient(HttpClientOptions options);
  UpbitHttpClient(HttpClientOptions options, std::unique_ptr<ITransport> transport);

  HttpResponse RequestJson(const HttpRequest& request);

 private:
  friend class DefaultHttpTransport;

  struct ParsedBaseUrl {
    std::string scheme = "https";
    std::string host;
    int port = 443;
    std::string base_path;
  };

  static std::string ToUpper(std::string value);
  static std::string Trim(std::string value);
  static std::pair<std::string, bool> ClassifyStatus(int status_code);
  static std::pair<std::string, std::string> ParseErrorPayload(
      int status_code, const std::string& body, const nlohmann::json& json_body);
  static int Extract418CooldownSec(
      const std::unordered_map<std::string, std::string>& headers,
      const std::string& error_message,
      int fallback);
  static ParsedBaseUrl ParseBaseUrl(const std::string& base_url);
  static std::string CanonicalHeaderLookup(
      const std::unordered_map<std::string, std::string>& headers, const std::string& key);
  static void SleepBackoff(int attempt, int base_ms, int max_ms);

  RawResponse PerformRequest(
      const std::string& method_upper,
      const std::string& endpoint,
      const std::string& encoded_query,
      const std::unordered_map<std::string, std::string>& headers,
      const std::string& body_json) const;

  ParsedBaseUrl base_url_;
  HttpClientOptions options_;
  UpbitRateLimiter limiter_;
  std::optional<UpbitJwtSigner> signer_;
  std::unique_ptr<ITransport> transport_;
};

}  // namespace autobot::executor::upbit
