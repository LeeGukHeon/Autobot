#include "upbit/http_client.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <regex>
#include <stdexcept>
#include <thread>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <winhttp.h>
#endif

namespace autobot::executor::upbit {

class DefaultHttpTransport final : public UpbitHttpClient::ITransport {
 public:
  explicit DefaultHttpTransport(UpbitHttpClient* owner) : owner_(owner) {}

  UpbitHttpClient::RawResponse PerformRequest(
      const std::string& method_upper,
      const std::string& endpoint,
      const std::string& encoded_query,
      const std::unordered_map<std::string, std::string>& headers,
      const std::string& body_json) override;

 private:
  UpbitHttpClient* owner_ = nullptr;
};

namespace {

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string TrimCopy(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

bool ParseBoolFlag(const std::string& value, bool fallback) {
  const std::string lowered = ToLower(TrimCopy(value));
  if (lowered.empty()) {
    return fallback;
  }
  if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "y" || lowered == "on") {
    return true;
  }
  if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "n" || lowered == "off") {
    return false;
  }
  return fallback;
}

bool DebugAuthEnabled() {
  const char* raw = std::getenv("AUTOBOT_EXECUTOR_DEBUG_AUTH");
  if (raw == nullptr) {
    return false;
  }
  return ParseBoolFlag(raw, false);
}

std::string HeaderValue(
    const std::unordered_map<std::string, std::string>& headers,
    std::string key) {
  const auto direct = headers.find(key);
  if (direct != headers.end()) {
    return direct->second;
  }
  key = ToLower(std::move(key));
  for (const auto& [header_key, header_value] : headers) {
    if (ToLower(header_key) == key) {
      return header_value;
    }
  }
  return "";
}

std::string TruncateForLog(std::string value, std::size_t max_len) {
  if (value.size() <= max_len) {
    return value;
  }
  value.resize(max_len);
  value += "...(truncated)";
  return value;
}

std::wstring Utf8ToWide(const std::string& value) {
#ifdef _WIN32
  if (value.empty()) {
    return L"";
  }
  const int size = MultiByteToWideChar(CP_UTF8, 0, value.c_str(), -1, nullptr, 0);
  if (size <= 0) {
    return L"";
  }
  std::wstring out(static_cast<std::size_t>(size - 1), L'\0');
  MultiByteToWideChar(CP_UTF8, 0, value.c_str(), -1, out.data(), size);
  return out;
#else
  (void)value;
  return L"";
#endif
}

std::string WideToUtf8(const std::wstring& value) {
#ifdef _WIN32
  if (value.empty()) {
    return "";
  }
  const int size = WideCharToMultiByte(CP_UTF8, 0, value.c_str(), -1, nullptr, 0, nullptr, nullptr);
  if (size <= 0) {
    return "";
  }
  std::string out(static_cast<std::size_t>(size - 1), '\0');
  WideCharToMultiByte(CP_UTF8, 0, value.c_str(), -1, out.data(), size, nullptr, nullptr);
  return out;
#else
  (void)value;
  return "";
#endif
}

#ifdef _WIN32
std::string WinHttpLastErrorString() {
  const DWORD code = GetLastError();
  if (code == 0) {
    return "unknown WinHTTP error";
  }
  LPSTR message = nullptr;
  const DWORD len = FormatMessageA(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr,
      code,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      reinterpret_cast<LPSTR>(&message),
      0,
      nullptr);
  std::string out = "WinHTTP error " + std::to_string(code);
  if (len > 0 && message != nullptr) {
    out += ": ";
    out += std::string(message, message + len);
    while (!out.empty() && (out.back() == '\r' || out.back() == '\n')) {
      out.pop_back();
    }
  }
  if (message != nullptr) {
    LocalFree(message);
  }
  return out;
}

std::string QueryHeader(HINTERNET request, const wchar_t* header_name) {
  DWORD size = 0;
  WinHttpQueryHeaders(
      request, WINHTTP_QUERY_CUSTOM, const_cast<wchar_t*>(header_name), WINHTTP_NO_OUTPUT_BUFFER, &size, nullptr);
  if (GetLastError() != ERROR_INSUFFICIENT_BUFFER || size == 0) {
    return "";
  }

  std::wstring buffer(size / sizeof(wchar_t), L'\0');
  if (!WinHttpQueryHeaders(
          request, WINHTTP_QUERY_CUSTOM, const_cast<wchar_t*>(header_name), buffer.data(), &size, nullptr)) {
    return "";
  }
  if (!buffer.empty() && buffer.back() == L'\0') {
    buffer.pop_back();
  }
  return WideToUtf8(buffer);
}
#endif

}  // namespace

UpbitHttpClient::UpbitHttpClient(HttpClientOptions options)
    : UpbitHttpClient(std::move(options), nullptr) {}

UpbitHttpClient::UpbitHttpClient(
    HttpClientOptions options,
    std::unique_ptr<ITransport> transport)
    : base_url_(ParseBaseUrl(options.base_url)),
      options_(std::move(options)),
      limiter_(
          options_.rate_limit_enabled,
          options_.ban_cooldown_sec,
          options_.group_rates),
      transport_(std::move(transport)) {
  if (!options_.access_key.empty() && !options_.secret_key.empty()) {
    signer_.emplace(options_.access_key, options_.secret_key);
  }
  if (!transport_) {
    transport_ = std::make_unique<DefaultHttpTransport>(this);
  }
}

HttpResponse UpbitHttpClient::RequestJson(const HttpRequest& request) {
  HttpResponse response;
  const std::string method_upper = ToUpper(request.method);
  const int attempts = request.allow_retry ? std::max(options_.max_attempts, 1) : 1;
  const bool debug_auth = DebugAuthEnabled();

  for (int attempt = 1; attempt <= attempts; ++attempt) {
    limiter_.Acquire(request.rate_limit_group);

    std::unordered_map<std::string, std::string> headers = request.headers;
    headers["Accept"] = "application/json";
    if (request.auth) {
      if (!signer_.has_value()) {
        response.ok = false;
        response.category = "auth";
        response.retriable = false;
        response.error_name = "missing_credentials";
        response.error_message = "Upbit private API requires access/secret key";
        return response;
      }
      headers["Authorization"] = signer_->BuildAuthorizationHeader(request.auth_query);
    }
    if (!request.body_json.empty()) {
      if (headers.find("Content-Type") == headers.end()) {
        headers["Content-Type"] = "application/json; charset=utf-8";
      }
    }

    if (debug_auth && request.auth) {
      const bool url_has_query = !request.url_query.empty();
      const std::string content_type = HeaderValue(headers, "Content-Type");
      const std::string lowered_content_type = ToLower(content_type);
      const bool content_type_json =
          lowered_content_type.find("application/json") != std::string::npos;
      const std::string query_hash =
          request.auth_query.empty() ? "" : UpbitJwtSigner::HashQueryString(request.auth_query);

      std::cerr << "[executor][auth_debug] attempt=" << attempt << "/" << attempts
                << " method=" << method_upper
                << " path=" << request.endpoint
                << " url_has_query=" << (url_has_query ? "true" : "false")
                << " body_len=" << request.body_json.size()
                << " content_type=" << (content_type.empty() ? "<none>" : content_type)
                << " content_type_json=" << (content_type_json ? "true" : "false")
                << std::endl;
      if (!request.body_json.empty()) {
        std::cerr << "[executor][auth_debug] body_json="
                  << TruncateForLog(request.body_json, 1024)
                  << std::endl;
      }
      std::cerr << "[executor][auth_debug] query_string_for_hash="
                << TruncateForLog(request.auth_query, 1024)
                << std::endl;
      std::cerr << "[executor][auth_debug] query_hash=" << query_hash
                << " len=" << query_hash.size()
                << " query_hash_alg=SHA512"
                << std::endl;
    }

    const RawResponse raw = transport_->PerformRequest(
        method_upper,
        request.endpoint,
        request.url_query,
        headers,
        request.body_json);
    if (!raw.network_ok) {
      if (attempt < attempts) {
        SleepBackoff(attempt, options_.base_backoff_ms, options_.max_backoff_ms);
        continue;
      }
      response.ok = false;
      response.status_code = 0;
      response.category = "network";
      response.retriable = true;
      response.error_name = "network_error";
      response.error_message = raw.network_error.empty() ? "network request failed" : raw.network_error;
      return response;
    }

    response.status_code = raw.status_code;
    response.raw_body = raw.body;
    response.request_id = CanonicalHeaderLookup(raw.headers, "request-id");
    if (response.request_id.empty()) {
      response.request_id = CanonicalHeaderLookup(raw.headers, "x-request-id");
    }
    response.remaining_req = ParseRemainingReqHeader(CanonicalHeaderLookup(raw.headers, "remaining-req"));
    limiter_.ObserveRemainingReq(response.remaining_req);
    if (!response.remaining_req.valid) {
      limiter_.ObserveMissingRemainingReq(request.rate_limit_group);
    }

    if (!raw.body.empty()) {
      try {
        response.json_body = nlohmann::json::parse(raw.body);
      } catch (...) {
        response.json_body = nlohmann::json::object();
      }
    } else {
      response.json_body = nlohmann::json::object();
    }

    if (raw.status_code >= 200 && raw.status_code < 300) {
      response.ok = true;
      response.category = "ok";
      response.retriable = false;
      return response;
    }

    const auto [error_name, error_message] =
        ParseErrorPayload(raw.status_code, raw.body, response.json_body);
    response.error_name = error_name;
    response.error_message = error_message;

    const std::string group = response.remaining_req.valid ? response.remaining_req.group : request.rate_limit_group;
    if (raw.status_code == 429) {
      const double cooldown = limiter_.Register429(group, attempt);
      response.category = "rate_limit";
      response.retriable = true;
      response.cooldown_sec = cooldown;
      response.breaker_state = "group";
      if (attempt < attempts) {
        continue;
      }
      return response;
    }
    if (raw.status_code == 418) {
      const int cooldown = Extract418CooldownSec(raw.headers, response.error_message, options_.ban_cooldown_sec);
      const double applied = limiter_.Register418(group, cooldown);
      response.category = "rate_limit";
      response.retriable = true;
      response.banned = true;
      response.cooldown_sec = applied;
      response.breaker_state = "global";
      return response;
    }

    const auto [category, retriable] = ClassifyStatus(raw.status_code);
    response.category = category;
    response.retriable = retriable;
    if (category == "server" && attempt < attempts) {
      SleepBackoff(attempt, options_.base_backoff_ms, options_.max_backoff_ms);
      continue;
    }
    return response;
  }

  response.ok = false;
  response.category = "network";
  response.retriable = true;
  response.error_name = "request_failed";
  response.error_message = "Upbit request failed after retries";
  return response;
}

UpbitHttpClient::RawResponse DefaultHttpTransport::PerformRequest(
    const std::string& method_upper,
    const std::string& endpoint,
    const std::string& encoded_query,
    const std::unordered_map<std::string, std::string>& headers,
    const std::string& body_json) {
  if (owner_ == nullptr) {
    UpbitHttpClient::RawResponse raw;
    raw.network_ok = false;
    raw.network_error = "default_transport_owner_missing";
    return raw;
  }
  return owner_->PerformRequest(method_upper, endpoint, encoded_query, headers, body_json);
}

std::string UpbitHttpClient::ToUpper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  return value;
}

std::string UpbitHttpClient::Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

std::pair<std::string, bool> UpbitHttpClient::ClassifyStatus(int status_code) {
  if (status_code == 429 || status_code == 418) {
    return {"rate_limit", true};
  }
  if (status_code >= 500) {
    return {"server", true};
  }
  if (status_code == 401) {
    return {"auth", false};
  }
  if (status_code == 400 || status_code == 404 || status_code == 422) {
    return {"validation", false};
  }
  if (status_code >= 400 && status_code < 500) {
    return {"client", false};
  }
  return {"ok", false};
}

std::pair<std::string, std::string> UpbitHttpClient::ParseErrorPayload(
    int status_code,
    const std::string& body,
    const nlohmann::json& json_body) {
  std::string name;
  std::string message;

  if (json_body.is_object()) {
    const auto error_it = json_body.find("error");
    if (error_it != json_body.end() && error_it->is_object()) {
      const auto nested_name = error_it->find("name");
      if (nested_name != error_it->end() && nested_name->is_string()) {
        name = nested_name->get<std::string>();
      }
      const auto nested_message = error_it->find("message");
      if (nested_message != error_it->end() && nested_message->is_string()) {
        message = nested_message->get<std::string>();
      }
    }
    if (message.empty()) {
      const auto msg_it = json_body.find("message");
      if (msg_it != json_body.end() && msg_it->is_string()) {
        message = msg_it->get<std::string>();
      }
    }
  }

  if (message.empty()) {
    if (!body.empty()) {
      message = body;
    } else {
      message = "HTTP " + std::to_string(status_code);
    }
  }
  return {name, message};
}

int UpbitHttpClient::Extract418CooldownSec(
    const std::unordered_map<std::string, std::string>& headers,
    const std::string& error_message,
    int fallback) {
  const std::string retry_after = CanonicalHeaderLookup(headers, "retry-after");
  if (!retry_after.empty()) {
    try {
      return std::max(static_cast<int>(std::ceil(std::stod(retry_after))), 1);
    } catch (...) {
      // fall through
    }
  }

  if (!error_message.empty()) {
    std::smatch match;
    if (std::regex_search(error_message, match, std::regex(R"((\d+))")) && match.size() > 1) {
      try {
        int value = std::max(std::stoi(match[1].str()), 1);
        const std::string lowered = ToLower(error_message);
        if (lowered.find("minute") != std::string::npos || lowered.find("min") != std::string::npos) {
          value *= 60;
        }
        return value;
      } catch (...) {
        // fall through
      }
    }
  }

  return std::max(fallback, 1);
}

UpbitHttpClient::ParsedBaseUrl UpbitHttpClient::ParseBaseUrl(const std::string& base_url) {
  ParsedBaseUrl parsed;
  std::string raw = Trim(base_url);
  if (raw.empty()) {
    raw = "https://api.upbit.com";
  }

  const std::size_t scheme_pos = raw.find("://");
  if (scheme_pos != std::string::npos) {
    parsed.scheme = ToLower(raw.substr(0, scheme_pos));
    raw = raw.substr(scheme_pos + 3);
  }

  const std::size_t path_pos = raw.find('/');
  std::string host_port = raw;
  if (path_pos != std::string::npos) {
    host_port = raw.substr(0, path_pos);
    parsed.base_path = raw.substr(path_pos);
  } else {
    parsed.base_path = "";
  }
  if (!parsed.base_path.empty() && parsed.base_path.back() == '/') {
    parsed.base_path.pop_back();
  }

  const std::size_t colon_pos = host_port.rfind(':');
  if (colon_pos != std::string::npos && colon_pos + 1 < host_port.size()) {
    parsed.host = host_port.substr(0, colon_pos);
    try {
      parsed.port = std::stoi(host_port.substr(colon_pos + 1));
    } catch (...) {
      throw std::runtime_error("invalid base_url port: " + base_url);
    }
  } else {
    parsed.host = host_port;
    parsed.port = (parsed.scheme == "http") ? 80 : 443;
  }

  parsed.host = Trim(parsed.host);
  if (parsed.host.empty()) {
    throw std::runtime_error("invalid base_url host: " + base_url);
  }
  if (parsed.scheme != "http" && parsed.scheme != "https") {
    throw std::runtime_error("unsupported base_url scheme: " + parsed.scheme);
  }
  return parsed;
}

std::string UpbitHttpClient::CanonicalHeaderLookup(
    const std::unordered_map<std::string, std::string>& headers, const std::string& key) {
  const auto found = headers.find(ToLower(key));
  if (found == headers.end()) {
    return "";
  }
  return found->second;
}

void UpbitHttpClient::SleepBackoff(int attempt, int base_ms, int max_ms) {
  const int sanitized_base = std::max(base_ms, 1);
  const int sanitized_max = std::max(max_ms, sanitized_base);
  const int delay_ms =
      std::min(sanitized_base * (1 << std::max(attempt - 1, 0)), sanitized_max);
  std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
}

UpbitHttpClient::RawResponse UpbitHttpClient::PerformRequest(
    const std::string& method_upper,
    const std::string& endpoint,
    const std::string& encoded_query,
    const std::unordered_map<std::string, std::string>& headers,
    const std::string& body_json) const {
  RawResponse raw;

#ifdef _WIN32
  const std::wstring user_agent = L"autobot_executor/1.0";
  const std::wstring host = Utf8ToWide(base_url_.host);
  const bool secure = base_url_.scheme == "https";

  std::string normalized_endpoint = endpoint;
  if (normalized_endpoint.empty() || normalized_endpoint.front() != '/') {
    normalized_endpoint = "/" + normalized_endpoint;
  }
  std::string path = base_url_.base_path + normalized_endpoint;
  if (!encoded_query.empty()) {
    path += "?" + encoded_query;
  }

  HINTERNET h_session = WinHttpOpen(
      user_agent.c_str(), WINHTTP_ACCESS_TYPE_DEFAULT_PROXY, WINHTTP_NO_PROXY_NAME, WINHTTP_NO_PROXY_BYPASS, 0);
  if (h_session == nullptr) {
    raw.network_error = WinHttpLastErrorString();
    return raw;
  }

  HINTERNET h_connect = WinHttpConnect(h_session, host.c_str(), static_cast<INTERNET_PORT>(base_url_.port), 0);
  if (h_connect == nullptr) {
    raw.network_error = WinHttpLastErrorString();
    WinHttpCloseHandle(h_session);
    return raw;
  }

  const std::wstring method_w = Utf8ToWide(method_upper);
  const std::wstring path_w = Utf8ToWide(path);
  HINTERNET h_request = WinHttpOpenRequest(
      h_connect,
      method_w.c_str(),
      path_w.c_str(),
      nullptr,
      WINHTTP_NO_REFERER,
      WINHTTP_DEFAULT_ACCEPT_TYPES,
      secure ? WINHTTP_FLAG_SECURE : 0);
  if (h_request == nullptr) {
    raw.network_error = WinHttpLastErrorString();
    WinHttpCloseHandle(h_connect);
    WinHttpCloseHandle(h_session);
    return raw;
  }

  WinHttpSetTimeouts(
      h_request,
      options_.connect_timeout_ms,
      options_.connect_timeout_ms,
      options_.write_timeout_ms,
      options_.read_timeout_ms);

  std::wstring header_lines;
  for (const auto& [key, value] : headers) {
    header_lines += Utf8ToWide(key);
    header_lines += L": ";
    header_lines += Utf8ToWide(value);
    header_lines += L"\r\n";
  }

  LPVOID optional_data = WINHTTP_NO_REQUEST_DATA;
  DWORD optional_len = 0;
  if (!body_json.empty()) {
    optional_data = const_cast<char*>(body_json.data());
    optional_len = static_cast<DWORD>(body_json.size());
  }

  const BOOL sent = WinHttpSendRequest(
      h_request,
      header_lines.empty() ? WINHTTP_NO_ADDITIONAL_HEADERS : header_lines.c_str(),
      header_lines.empty() ? 0 : static_cast<DWORD>(-1L),
      optional_data,
      optional_len,
      optional_len,
      0);
  if (!sent) {
    raw.network_error = WinHttpLastErrorString();
    WinHttpCloseHandle(h_request);
    WinHttpCloseHandle(h_connect);
    WinHttpCloseHandle(h_session);
    return raw;
  }

  if (!WinHttpReceiveResponse(h_request, nullptr)) {
    raw.network_error = WinHttpLastErrorString();
    WinHttpCloseHandle(h_request);
    WinHttpCloseHandle(h_connect);
    WinHttpCloseHandle(h_session);
    return raw;
  }

  DWORD status_code = 0;
  DWORD status_size = sizeof(status_code);
  if (!WinHttpQueryHeaders(
          h_request,
          WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
          WINHTTP_HEADER_NAME_BY_INDEX,
          &status_code,
          &status_size,
          WINHTTP_NO_HEADER_INDEX)) {
    raw.network_error = WinHttpLastErrorString();
    WinHttpCloseHandle(h_request);
    WinHttpCloseHandle(h_connect);
    WinHttpCloseHandle(h_session);
    return raw;
  }
  raw.status_code = static_cast<int>(status_code);

  raw.headers["remaining-req"] = QueryHeader(h_request, L"Remaining-Req");
  raw.headers["request-id"] = QueryHeader(h_request, L"Request-Id");
  raw.headers["x-request-id"] = QueryHeader(h_request, L"X-Request-Id");
  raw.headers["retry-after"] = QueryHeader(h_request, L"Retry-After");
  raw.headers["content-type"] = QueryHeader(h_request, L"Content-Type");

  std::string body;
  while (true) {
    DWORD available = 0;
    if (!WinHttpQueryDataAvailable(h_request, &available)) {
      raw.network_error = WinHttpLastErrorString();
      WinHttpCloseHandle(h_request);
      WinHttpCloseHandle(h_connect);
      WinHttpCloseHandle(h_session);
      return raw;
    }
    if (available == 0) {
      break;
    }

    std::string chunk(available, '\0');
    DWORD read = 0;
    if (!WinHttpReadData(h_request, chunk.data(), available, &read)) {
      raw.network_error = WinHttpLastErrorString();
      WinHttpCloseHandle(h_request);
      WinHttpCloseHandle(h_connect);
      WinHttpCloseHandle(h_session);
      return raw;
    }
    if (read == 0) {
      break;
    }
    chunk.resize(read);
    body += chunk;
  }
  raw.body = std::move(body);

  WinHttpCloseHandle(h_request);
  WinHttpCloseHandle(h_connect);
  WinHttpCloseHandle(h_session);
  raw.network_ok = true;
  return raw;
#else
  (void)method_upper;
  (void)endpoint;
  (void)encoded_query;
  (void)headers;
  (void)body_json;
  raw.network_ok = false;
  raw.network_error = "UpbitHttpClient requires WinHTTP on this build target";
  return raw;
#endif
}

}  // namespace autobot::executor::upbit

