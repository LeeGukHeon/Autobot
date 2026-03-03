#include "upbit/ws_private_client.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <deque>
#include <random>
#include <sstream>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <winhttp.h>
#endif

namespace autobot::executor::upbit {

namespace {

using SteadyClock = std::chrono::steady_clock;

std::int64_t NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

std::string Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string ToUpper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  return value;
}

WsKeepaliveMode ParseKeepaliveMode(const std::string& raw) {
  const std::string mode = ToLower(Trim(raw));
  if (mode == "off") {
    return WsKeepaliveMode::kOff;
  }
  if (mode == "frame") {
    return WsKeepaliveMode::kFrame;
  }
  return WsKeepaliveMode::kMessage;
}

class SlidingWindowLimiter {
 public:
  SlidingWindowLimiter(int per_second, int per_minute)
      : per_second_(std::max(per_second, 0)), per_minute_(std::max(per_minute, 0)) {}

  bool Acquire(const std::atomic<bool>* stop_flag) {
    while (true) {
      if (stop_flag != nullptr && stop_flag->load()) {
        return false;
      }

      std::chrono::milliseconds wait_ms(0);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now = SteadyClock::now();
        Prune(now);

        const bool sec_ok = per_second_ <= 0 || second_window_.size() < static_cast<std::size_t>(per_second_);
        const bool min_ok = per_minute_ <= 0 || minute_window_.size() < static_cast<std::size_t>(per_minute_);
        if (sec_ok && min_ok) {
          second_window_.push_back(now);
          if (per_minute_ > 0) {
            minute_window_.push_back(now);
          }
          return true;
        }

        if (!sec_ok && !second_window_.empty()) {
          wait_ms = std::max(
              wait_ms,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::seconds(1) - (now - second_window_.front())));
        }
        if (!min_ok && !minute_window_.empty()) {
          wait_ms = std::max(
              wait_ms,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::minutes(1) - (now - minute_window_.front())));
        }
      }

      if (wait_ms < std::chrono::milliseconds(20)) {
        wait_ms = std::chrono::milliseconds(20);
      }
      std::this_thread::sleep_for(wait_ms);
    }
  }

 private:
  void Prune(const SteadyClock::time_point& now) {
    while (!second_window_.empty() &&
           now - second_window_.front() >= std::chrono::seconds(1)) {
      second_window_.pop_front();
    }
    while (!minute_window_.empty() &&
           now - minute_window_.front() >= std::chrono::minutes(1)) {
      minute_window_.pop_front();
    }
  }

  int per_second_;
  int per_minute_;
  std::deque<SteadyClock::time_point> second_window_;
  std::deque<SteadyClock::time_point> minute_window_;
  std::mutex mutex_;
};

#ifdef _WIN32

struct ParsedWsUrl {
  bool secure = true;
  std::string host;
  int port = 443;
  std::string path_query = "/";
};

std::wstring Utf8ToWide(const std::string& value) {
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
}

std::string WideToUtf8(const std::wstring& value) {
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
}

std::string WinErrorToString(DWORD code) {
  LPWSTR message = nullptr;
  const DWORD len = FormatMessageW(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr,
      code,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      reinterpret_cast<LPWSTR>(&message),
      0,
      nullptr);
  std::string out = "WinHTTP error " + std::to_string(code);
  if (len > 0 && message != nullptr) {
    std::wstring trimmed(message, message + len);
    while (!trimmed.empty() && (trimmed.back() == L'\r' || trimmed.back() == L'\n')) {
      trimmed.pop_back();
    }
    out += ": " + WideToUtf8(trimmed);
  }
  if (message != nullptr) {
    LocalFree(message);
  }
  return out;
}

class ScopedInternetHandle {
 public:
  explicit ScopedInternetHandle(HINTERNET handle = nullptr) : handle_(handle) {}
  ~ScopedInternetHandle() {
    if (handle_ != nullptr) {
      WinHttpCloseHandle(handle_);
    }
  }
  ScopedInternetHandle(const ScopedInternetHandle&) = delete;
  ScopedInternetHandle& operator=(const ScopedInternetHandle&) = delete;
  ScopedInternetHandle(ScopedInternetHandle&& other) noexcept : handle_(other.handle_) {
    other.handle_ = nullptr;
  }
  ScopedInternetHandle& operator=(ScopedInternetHandle&& other) noexcept {
    if (this != &other) {
      if (handle_ != nullptr) {
        WinHttpCloseHandle(handle_);
      }
      handle_ = other.handle_;
      other.handle_ = nullptr;
    }
    return *this;
  }
  HINTERNET get() const { return handle_; }
  HINTERNET release() {
    HINTERNET tmp = handle_;
    handle_ = nullptr;
    return tmp;
  }
  explicit operator bool() const { return handle_ != nullptr; }

 private:
  HINTERNET handle_ = nullptr;
};

bool ParseWsUrl(const std::string& raw, ParsedWsUrl* out, std::string* error) {
  if (out == nullptr) {
    if (error != nullptr) {
      *error = "internal_error_null_url_out";
    }
    return false;
  }
  std::string value = Trim(raw);
  if (value.empty()) {
    if (error != nullptr) {
      *error = "ws_url is empty";
    }
    return false;
  }
  const std::size_t scheme_pos = value.find("://");
  if (scheme_pos == std::string::npos) {
    if (error != nullptr) {
      *error = "ws_url missing scheme";
    }
    return false;
  }

  const std::string scheme = ToLower(value.substr(0, scheme_pos));
  if (scheme != "ws" && scheme != "wss") {
    if (error != nullptr) {
      *error = "unsupported ws scheme: " + scheme;
    }
    return false;
  }
  out->secure = scheme == "wss";

  std::string remainder = value.substr(scheme_pos + 3);
  const std::size_t path_pos = remainder.find('/');
  std::string host_port = remainder;
  out->path_query = "/";
  if (path_pos != std::string::npos) {
    host_port = remainder.substr(0, path_pos);
    out->path_query = remainder.substr(path_pos);
  }

  const std::size_t colon_pos = host_port.rfind(':');
  if (colon_pos != std::string::npos && colon_pos + 1 < host_port.size()) {
    out->host = host_port.substr(0, colon_pos);
    try {
      out->port = std::stoi(host_port.substr(colon_pos + 1));
    } catch (...) {
      if (error != nullptr) {
        *error = "invalid ws port";
      }
      return false;
    }
  } else {
    out->host = host_port;
    out->port = out->secure ? 443 : 80;
  }
  out->host = Trim(out->host);
  if (out->host.empty()) {
    if (error != nullptr) {
      *error = "ws host is empty";
    }
    return false;
  }
  if (out->path_query.empty()) {
    out->path_query = "/";
  }
  if (error != nullptr) {
    error->clear();
  }
  return true;
}

bool SendUtf8Message(
    HINTERNET websocket,
    const std::string& message,
    SlidingWindowLimiter* send_limiter,
    const std::atomic<bool>* stop_flag,
    const std::function<void()>& on_tx,
    std::string* error) {
  if (websocket == nullptr || send_limiter == nullptr) {
    if (error != nullptr) {
      *error = "internal_error_invalid_send_state";
    }
    return false;
  }
  if (!send_limiter->Acquire(stop_flag)) {
    if (error != nullptr) {
      *error = "stopped_before_send";
    }
    return false;
  }
  DWORD status = WinHttpWebSocketSend(
      websocket,
      WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE,
      reinterpret_cast<PVOID>(const_cast<char*>(message.data())),
      static_cast<DWORD>(message.size()));
  if (status != NO_ERROR) {
    if (error != nullptr) {
      *error = WinErrorToString(status);
    }
    return false;
  }
  if (on_tx) {
    on_tx();
  }
  return true;
}

bool SendPingFrame(
    HINTERNET websocket,
    SlidingWindowLimiter* send_limiter,
    const std::atomic<bool>* stop_flag,
    const std::function<void()>& on_tx,
    std::string* error) {
  if (websocket == nullptr || send_limiter == nullptr) {
    if (error != nullptr) {
      *error = "internal_error_invalid_send_state";
    }
    return false;
  }
  if (!send_limiter->Acquire(stop_flag)) {
    if (error != nullptr) {
      *error = "stopped_before_send";
    }
    return false;
  }

#ifdef WINHTTP_WEB_SOCKET_PING_BUFFER_TYPE
  const DWORD status = WinHttpWebSocketSend(
      websocket,
      WINHTTP_WEB_SOCKET_PING_BUFFER_TYPE,
      nullptr,
      0);
  if (status != NO_ERROR) {
    if (error != nullptr) {
      *error = WinErrorToString(status);
    }
    return false;
  }
  if (on_tx) {
    on_tx();
  }
  return true;
#else
  if (error != nullptr) {
    *error = "ws_ping_frame_not_supported_by_winhttp_sdk";
  }
  return false;
#endif
}

bool SendPingByMode(
    HINTERNET websocket,
    WsKeepaliveMode mode,
    SlidingWindowLimiter* send_limiter,
    const std::atomic<bool>* stop_flag,
    std::mutex* send_mutex,
    const std::function<void()>& on_tx,
    const std::function<void()>& on_ping_sent,
    std::string* error) {
  if (send_mutex == nullptr) {
    if (error != nullptr) {
      *error = "internal_error_null_send_mutex";
    }
    return false;
  }
  std::lock_guard<std::mutex> send_lock(*send_mutex);

  bool sent = false;
  if (mode == WsKeepaliveMode::kFrame) {
    sent = SendPingFrame(websocket, send_limiter, stop_flag, on_tx, error);
  } else {
    sent = SendUtf8Message(websocket, "PING", send_limiter, stop_flag, on_tx, error);
  }
  if (!sent) {
    return false;
  }
  if (on_ping_sent) {
    on_ping_sent();
  }
  return true;
}

bool SendTextThreadSafe(
    HINTERNET websocket,
    const std::string& message,
    SlidingWindowLimiter* send_limiter,
    const std::atomic<bool>* stop_flag,
    std::mutex* send_mutex,
    const std::function<void()>& on_tx,
    std::string* error) {
  if (send_mutex == nullptr) {
    if (error != nullptr) {
      *error = "internal_error_null_send_mutex";
    }
    return false;
  }
  std::lock_guard<std::mutex> send_lock(*send_mutex);
  return SendUtf8Message(websocket, message, send_limiter, stop_flag, on_tx, error);
}

bool ReceiveMessage(
    HINTERNET websocket,
    std::string* out_message,
    bool* out_remote_close,
    bool* out_timeout,
    std::string* error) {
  if (out_message == nullptr || out_remote_close == nullptr || out_timeout == nullptr) {
    if (error != nullptr) {
      *error = "internal_error_invalid_receive_out";
    }
    return false;
  }
  *out_message = "";
  *out_remote_close = false;
  *out_timeout = false;

  std::vector<char> buffer(16 * 1024, '\0');
  while (true) {
    DWORD bytes_read = 0;
    WINHTTP_WEB_SOCKET_BUFFER_TYPE buffer_type = WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE;
    const DWORD status = WinHttpWebSocketReceive(
        websocket,
        buffer.data(),
        static_cast<DWORD>(buffer.size()),
        &bytes_read,
        &buffer_type);
    if (status != NO_ERROR) {
      if (status == ERROR_WINHTTP_TIMEOUT) {
        *out_timeout = true;
        return true;
      }
      if (error != nullptr) {
        *error = WinErrorToString(status);
      }
      return false;
    }

    if (buffer_type == WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE) {
      *out_remote_close = true;
      return true;
    }

    if (buffer_type == WINHTTP_WEB_SOCKET_BINARY_FRAGMENT_BUFFER_TYPE ||
        buffer_type == WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE ||
        buffer_type == WINHTTP_WEB_SOCKET_UTF8_FRAGMENT_BUFFER_TYPE ||
        buffer_type == WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE) {
      if (bytes_read > 0) {
        out_message->append(buffer.data(), buffer.data() + bytes_read);
      }
      if (buffer_type == WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE ||
          buffer_type == WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE) {
        return true;
      }
      continue;
    }

    return true;
  }
}

bool SleepInterruptible(int delay_ms, const std::atomic<bool>* stop_flag) {
  const int bounded_ms = std::max(delay_ms, 0);
  int elapsed = 0;
  while (elapsed < bounded_ms) {
    if (stop_flag != nullptr && stop_flag->load()) {
      return false;
    }
    const int step = std::min(100, bounded_ms - elapsed);
    std::this_thread::sleep_for(std::chrono::milliseconds(step));
    elapsed += step;
  }
  return true;
}

std::string BuildTicket(const std::string& prefix) {
  return prefix + "-" + std::to_string(NowMs());
}

bool ExtractWsError(
    const std::string& message,
    std::string* error_name,
    std::string* error_message) {
  if (error_name != nullptr) {
    error_name->clear();
  }
  if (error_message != nullptr) {
    error_message->clear();
  }

  const nlohmann::json parsed = nlohmann::json::parse(message, nullptr, false);
  if (!parsed.is_object()) {
    return false;
  }
  auto extract_str = [](const nlohmann::json& obj, const char* key) -> std::string {
    const auto found = obj.find(key);
    if (found == obj.end() || !found->is_string()) {
      return "";
    }
    return found->get<std::string>();
  };

  std::string name = extract_str(parsed, "name");
  std::string text = extract_str(parsed, "message");
  const auto nested = parsed.find("error");
  if (nested != parsed.end() && nested->is_object()) {
    if (name.empty()) {
      name = extract_str(*nested, "name");
    }
    if (text.empty()) {
      text = extract_str(*nested, "message");
    }
  }

  if (name.empty() && text.empty()) {
    return false;
  }
  if (error_name != nullptr) {
    *error_name = std::move(name);
  }
  if (error_message != nullptr) {
    *error_message = std::move(text);
  }
  return true;
}

bool ConnectAndConsume(
    const WsPrivateClientOptions& options,
    const std::string& authorization_header,
    SlidingWindowLimiter* send_limiter,
    const std::atomic<bool>* stop_flag,
    const std::function<void()>& on_tx,
    const std::function<void()>& on_rx,
    const std::function<void()>& on_ping_sent,
    const std::function<void()>& on_pong_rx,
    const std::function<void()>& on_stale_disconnect,
    const UpbitPrivateWsClient::Callbacks& callbacks,
    std::string* disconnect_reason) {
  if (disconnect_reason != nullptr) {
    disconnect_reason->clear();
  }
  std::string url_error;
  ParsedWsUrl url;
  if (!ParseWsUrl(options.url, &url, &url_error)) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = url_error;
    }
    return false;
  }

  const std::wstring user_agent = L"autobot_executor_private_ws/1.0";
  const std::wstring host_w = Utf8ToWide(url.host);
  const std::wstring path_w = Utf8ToWide(url.path_query);
  const std::wstring auth_header_w = Utf8ToWide("Authorization: " + authorization_header + "\r\n");

  ScopedInternetHandle session(WinHttpOpen(
      user_agent.c_str(),
      WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
      WINHTTP_NO_PROXY_NAME,
      WINHTTP_NO_PROXY_BYPASS,
      0));
  if (!session) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }

  ScopedInternetHandle connect(WinHttpConnect(
      session.get(), host_w.c_str(), static_cast<INTERNET_PORT>(url.port), 0));
  if (!connect) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }

  ScopedInternetHandle request(WinHttpOpenRequest(
      connect.get(),
      L"GET",
      path_w.c_str(),
      nullptr,
      WINHTTP_NO_REFERER,
      WINHTTP_DEFAULT_ACCEPT_TYPES,
      url.secure ? WINHTTP_FLAG_SECURE : 0));
  if (!request) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }

  WinHttpSetTimeouts(
      request.get(),
      options.connect_timeout_ms,
      options.connect_timeout_ms,
      options.write_timeout_ms,
      options.read_timeout_ms);

  if (!WinHttpSetOption(request.get(), WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET, nullptr, 0)) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }

  if (!WinHttpSendRequest(
          request.get(),
          auth_header_w.empty() ? WINHTTP_NO_ADDITIONAL_HEADERS : auth_header_w.c_str(),
          auth_header_w.empty() ? 0 : static_cast<DWORD>(-1L),
          WINHTTP_NO_REQUEST_DATA,
          0,
          0,
          0)) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }
  if (!WinHttpReceiveResponse(request.get(), nullptr)) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }

  DWORD status_code = 0;
  DWORD status_size = sizeof(status_code);
  if (!WinHttpQueryHeaders(
          request.get(),
          WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
          WINHTTP_HEADER_NAME_BY_INDEX,
          &status_code,
          &status_size,
          WINHTTP_NO_HEADER_INDEX)) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }
  if (status_code != 101) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = "ws_upgrade_failed_status_" + std::to_string(status_code);
    }
    return false;
  }

  ScopedInternetHandle websocket(WinHttpWebSocketCompleteUpgrade(request.get(), 0));
  if (!websocket) {
    if (disconnect_reason != nullptr) {
      *disconnect_reason = WinErrorToString(GetLastError());
    }
    return false;
  }
  request = ScopedInternetHandle();

  if (callbacks.on_connected) {
    callbacks.on_connected();
  }

  std::atomic<std::int64_t> last_rx_ts_ms(NowMs());
  std::atomic<std::int64_t> last_tx_ts_ms(NowMs());
  std::atomic<std::int64_t> last_ping_ts_ms(0);
  std::atomic<std::int64_t> last_pong_ts_ms(0);
  std::atomic<bool> connection_active(true);
  std::atomic<bool> stale_disconnect_requested(false);
  std::mutex send_mutex;
  std::mutex reason_mutex;
  std::string resolved_disconnect_reason;
  bool disconnect_reason_set = false;
  auto set_disconnect_reason = [&](const std::string& reason) {
    std::lock_guard<std::mutex> lock(reason_mutex);
    if (!disconnect_reason_set) {
      resolved_disconnect_reason = reason;
      disconnect_reason_set = true;
    }
  };
  auto mark_rx = [&]() {
    last_rx_ts_ms.store(NowMs());
    if (on_rx) {
      on_rx();
    }
  };
  auto mark_tx = [&]() {
    last_tx_ts_ms.store(NowMs());
    if (on_tx) {
      on_tx();
    }
  };
  auto mark_ping_sent = [&]() {
    last_ping_ts_ms.store(NowMs());
    if (on_ping_sent) {
      on_ping_sent();
    }
  };
  auto mark_pong_rx = [&]() {
    last_pong_ts_ms.store(NowMs());
    if (on_pong_rx) {
      on_pong_rx();
    }
  };

  std::string current_subscribe_format = ToUpper(Trim(options.subscribe_format));
  if (current_subscribe_format.empty()) {
    current_subscribe_format = "DEFAULT";
  }
  bool format_fallback_used = false;
  auto send_subscribe = [&](const std::string& format) -> bool {
    std::string subscribe_error;
    const std::string subscribe_payload = BuildPrivateSubscribePayload(
        BuildTicket(options.ticket_prefix),
        options.subscribe_my_order,
        options.subscribe_my_asset,
        format,
        options.my_order_codes,
        &subscribe_error);
    if (subscribe_payload.empty()) {
      if (disconnect_reason != nullptr) {
        *disconnect_reason = subscribe_error.empty() ? "failed_to_build_subscribe_payload" : subscribe_error;
      }
      return false;
    }
    if (!SendTextThreadSafe(
            websocket.get(),
            subscribe_payload,
            send_limiter,
            stop_flag,
            &send_mutex,
            mark_tx,
            &subscribe_error)) {
      if (disconnect_reason != nullptr) {
        *disconnect_reason = "failed_to_send_subscribe: " + subscribe_error;
      }
      return false;
    }
    return true;
  };
  if (!send_subscribe(current_subscribe_format)) {
    return false;
  }

  WsKeepaliveSchedulerOptions keepalive_options;
  keepalive_options.mode = ParseKeepaliveMode(options.keepalive_mode);
  keepalive_options.ping_on_connect = options.ping_on_connect;
  keepalive_options.ping_interval_sec = std::max(options.ping_interval_sec, 1);
  keepalive_options.pong_grace_sec = std::max(options.pong_grace_sec, 1);
  keepalive_options.force_reconnect_on_stale = options.force_reconnect_on_stale;
  keepalive_options.stale_rx_threshold_sec = std::max(options.stale_rx_threshold_sec, 1);

  WsKeepaliveScheduler::Callbacks keepalive_callbacks;
  keepalive_callbacks.is_connected = [&connection_active, &stale_disconnect_requested]() {
    return connection_active.load() && !stale_disconnect_requested.load();
  };
  keepalive_callbacks.last_rx_ts_ms = [&last_rx_ts_ms]() { return last_rx_ts_ms.load(); };
  keepalive_callbacks.last_tx_ts_ms = [&last_tx_ts_ms]() { return last_tx_ts_ms.load(); };
  keepalive_callbacks.last_ping_ts_ms = [&last_ping_ts_ms]() { return last_ping_ts_ms.load(); };
  keepalive_callbacks.last_pong_ts_ms = [&last_pong_ts_ms]() { return last_pong_ts_ms.load(); };
  keepalive_callbacks.send_ping =
      [&](
          WsKeepaliveMode mode,
          std::string* error) {
        return SendPingByMode(
            websocket.get(),
            mode,
            send_limiter,
            stop_flag,
            &send_mutex,
            mark_tx,
            mark_ping_sent,
            error);
      };
  keepalive_callbacks.on_error = [&callbacks](const std::string& reason) {
    if (callbacks.on_error) {
      callbacks.on_error(reason);
    }
  };
  keepalive_callbacks.on_stale =
      [&](const std::string& reason) {
        stale_disconnect_requested.store(true);
        if (on_stale_disconnect) {
          on_stale_disconnect();
        }
        set_disconnect_reason("ws_keepalive_stale:" + reason);
        if (callbacks.on_error) {
          callbacks.on_error("ws_keepalive_stale:" + reason);
        }
        if (options.force_reconnect_on_stale) {
          WinHttpWebSocketClose(
              websocket.get(),
              WINHTTP_WEB_SOCKET_SUCCESS_CLOSE_STATUS,
              nullptr,
              0);
        }
      };
  WsKeepaliveScheduler keepalive_scheduler(
      keepalive_options,
      std::move(keepalive_callbacks));
  keepalive_scheduler.Start(stop_flag);

  while (stop_flag == nullptr || !stop_flag->load()) {
    if (stale_disconnect_requested.load()) {
      break;
    }

    std::string message;
    bool remote_close = false;
    bool timeout = false;
    std::string receive_error;
    if (!ReceiveMessage(websocket.get(), &message, &remote_close, &timeout, &receive_error)) {
      set_disconnect_reason("ws_receive_failed: " + receive_error);
      break;
    }

    if (remote_close) {
      set_disconnect_reason("ws_remote_closed");
      break;
    }
    if (timeout) {
      continue;
    }

    if (!message.empty()) {
      mark_rx();
    }
    if (message.empty()) {
      continue;
    }

    const std::string normalized = ToUpper(Trim(message));
    if (normalized == "PONG") {
      mark_pong_rx();
      continue;
    }
    if (normalized == "PING") {
      std::string pong_error;
      if (!SendTextThreadSafe(
              websocket.get(),
              "PONG",
              send_limiter,
              stop_flag,
              &send_mutex,
              mark_tx,
              &pong_error)) {
        set_disconnect_reason("ws_pong_failed: " + pong_error);
        break;
      }
      continue;
    }

    std::string ws_error_name;
    std::string ws_error_message;
    if (ExtractWsError(message, &ws_error_name, &ws_error_message)) {
      const std::string normalized_error = ToUpper(Trim(ws_error_name));
      if (normalized_error == "WRONG_FORMAT" &&
          options.format_fallback_to_default_once &&
          !format_fallback_used &&
          current_subscribe_format != "DEFAULT") {
        format_fallback_used = true;
        current_subscribe_format = "DEFAULT";
        if (!send_subscribe(current_subscribe_format)) {
          set_disconnect_reason("failed_to_send_subscribe");
          break;
        }
        continue;
      }
      std::string reason = "ws_server_error:" + ws_error_name;
      if (!ws_error_message.empty()) {
        reason += ":" + ws_error_message;
      }
      set_disconnect_reason(reason);
      break;
    }

    WsPrivateEvent parsed;
    if (!ParsePrivateWsMessage(message, &parsed)) {
      continue;
    }
    if (parsed.kind == WsPrivateEventKind::kMyOrder) {
      if (callbacks.on_my_order) {
        callbacks.on_my_order(parsed.order);
      }
      continue;
    }
    if (parsed.kind == WsPrivateEventKind::kMyAsset) {
      if (callbacks.on_my_asset) {
        callbacks.on_my_asset(parsed.asset);
      }
      continue;
    }
    if (parsed.kind == WsPrivateEventKind::kHealthUp) {
      mark_pong_rx();
      if (callbacks.on_health_up && options.up_status_log) {
        callbacks.on_health_up(parsed.health_up);
      }
      continue;
    }
  }

  connection_active.store(false);
  keepalive_scheduler.Stop();
  WinHttpWebSocketClose(
      websocket.get(),
      WINHTTP_WEB_SOCKET_SUCCESS_CLOSE_STATUS,
      nullptr,
      0);
  {
    std::lock_guard<std::mutex> lock(reason_mutex);
    if (!disconnect_reason_set) {
      resolved_disconnect_reason = (stop_flag != nullptr && stop_flag->load()) ? "stopped" : "private_ws_disconnected";
      disconnect_reason_set = true;
    }
  }
  if (disconnect_reason != nullptr) {
    *disconnect_reason = resolved_disconnect_reason;
  }
  return true;
}

#endif

}  // namespace

UpbitPrivateWsClient::UpbitPrivateWsClient(WsPrivateClientOptions options)
    : options_(std::move(options)) {
  options_.ticket_prefix = Trim(options_.ticket_prefix);
  if (options_.ticket_prefix.empty()) {
    options_.ticket_prefix = "autobot-executor-private";
  }
  options_.keepalive_mode = ToLower(Trim(options_.keepalive_mode));
  if (options_.keepalive_mode != "off" &&
      options_.keepalive_mode != "frame" &&
      options_.keepalive_mode != "message") {
    options_.keepalive_mode = "message";
  }
  options_.ping_interval_sec = std::max(options_.ping_interval_sec, 1);
  options_.pong_grace_sec = std::max(options_.pong_grace_sec, 1);
  options_.stale_rx_threshold_sec = std::max(options_.stale_rx_threshold_sec, 1);

  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.keepalive_mode = options_.keepalive_mode;
  stats_.keepalive_ping_interval_sec = options_.ping_interval_sec;
  stats_.keepalive_pong_grace_sec = options_.pong_grace_sec;
  stats_.keepalive_ping_on_connect = options_.ping_on_connect;
  stats_.keepalive_force_reconnect_on_stale = options_.force_reconnect_on_stale;
  stats_.keepalive_up_status_log = options_.up_status_log;
}

void UpbitPrivateWsClient::Run(
    const std::function<std::string()>& authorization_header_provider,
    const std::atomic<bool>* stop_flag,
    const Callbacks& callbacks) {
  if (!authorization_header_provider) {
    EmitError(callbacks, "private_ws_authorization_provider_missing");
    return;
  }
  if (!options_.subscribe_my_order && !options_.subscribe_my_asset) {
    EmitError(callbacks, "private_ws_no_stream_type_enabled");
    return;
  }

#ifdef _WIN32
  SlidingWindowLimiter connect_limiter(options_.connect_rps, 0);
  SlidingWindowLimiter send_limiter(options_.message_rps, options_.message_rpm);
  int reconnect_attempt = 0;

  Callbacks wrapped = callbacks;
  wrapped.on_connected = [this, &callbacks]() {
    MarkConnected();
    if (callbacks.on_connected) {
      callbacks.on_connected();
    }
  };
  wrapped.on_my_order = [this, &callbacks](const WsMyOrderEvent& event) {
    MarkReceivedEvent(event.ts_ms);
    if (callbacks.on_my_order) {
      callbacks.on_my_order(event);
    }
  };
  wrapped.on_my_asset = [this, &callbacks](const WsMyAssetEvent& event) {
    MarkReceivedEvent(event.ts_ms);
    if (callbacks.on_my_asset) {
      callbacks.on_my_asset(event);
    }
  };
  wrapped.on_health_up = [this, &callbacks](const WsHealthUpEvent& event) {
    MarkReceivedEvent(event.ts_ms);
    if (callbacks.on_health_up) {
      callbacks.on_health_up(event);
    }
  };
  const auto on_tx = [this]() { MarkTx(); };
  const auto on_rx = [this]() { MarkRx(); };
  const auto on_ping_sent = [this]() { MarkPingSent(); };
  const auto on_pong_rx = [this]() { MarkPongReceived(); };
  const auto on_stale_disconnect = [this]() { MarkStaleDisconnect(); };

  while (stop_flag == nullptr || !stop_flag->load()) {
    if (!connect_limiter.Acquire(stop_flag)) {
      break;
    }

    const std::string authorization_header = Trim(authorization_header_provider());
    std::string disconnect_reason;
    bool had_connection = false;
    if (authorization_header.empty()) {
      disconnect_reason = "private_ws_authorization_header_is_empty";
    } else {
      had_connection = ConnectAndConsume(
          options_,
          authorization_header,
          &send_limiter,
          stop_flag,
          on_tx,
          on_rx,
          on_ping_sent,
          on_pong_rx,
          on_stale_disconnect,
          wrapped,
          &disconnect_reason);
    }

    if (had_connection) {
      reconnect_attempt = 0;
    }
    if (disconnect_reason.empty()) {
      disconnect_reason = "private_ws_disconnected";
    }
    MarkDisconnected(disconnect_reason);
    if (callbacks.on_disconnected) {
      callbacks.on_disconnected(disconnect_reason);
    }

    if (stop_flag != nullptr && stop_flag->load()) {
      break;
    }
    if (!options_.reconnect_enabled) {
      break;
    }

    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      stats_.reconnect_count += 1;
    }
    const int delay_ms = ReconnectDelayMs(reconnect_attempt++);
    if (!SleepInterruptible(delay_ms, stop_flag)) {
      break;
    }
  }
#else
  (void)stop_flag;
  EmitError(callbacks, "private_ws_client_requires_windows_winhttp_websocket");
#endif
}

WsPrivateClientStats UpbitPrivateWsClient::Stats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void UpbitPrivateWsClient::EmitError(const Callbacks& callbacks, const std::string& reason) const {
  if (callbacks.on_error) {
    callbacks.on_error(reason);
  }
}

void UpbitPrivateWsClient::MarkConnected() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.last_connect_ts_ms = NowMs();
}

void UpbitPrivateWsClient::MarkDisconnected(const std::string& reason) {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.last_disconnect_ts_ms = NowMs();
  stats_.last_disconnect_reason = reason;
}

void UpbitPrivateWsClient::MarkReceivedEvent(std::int64_t event_ts_ms) {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.received_events += 1;
  stats_.last_event_ts_ms = event_ts_ms;
}

void UpbitPrivateWsClient::MarkRx() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.last_rx_ts_ms = NowMs();
}

void UpbitPrivateWsClient::MarkTx() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.last_tx_ts_ms = NowMs();
}

void UpbitPrivateWsClient::MarkPingSent() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.ping_sent_count += 1;
  stats_.last_ping_ts_ms = NowMs();
}

void UpbitPrivateWsClient::MarkPongReceived() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.pong_rx_count += 1;
  stats_.last_pong_ts_ms = NowMs();
}

void UpbitPrivateWsClient::MarkStaleDisconnect() {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats_.stale_disconnect_count += 1;
}

int UpbitPrivateWsClient::ReconnectDelayMs(int attempt_index) const {
  const int bounded_attempt = std::max(attempt_index, 0);
  const int base = std::max(options_.reconnect_base_delay_ms, 1);
  const int max_delay = std::max(options_.reconnect_max_delay_ms, base);
  int backoff = base;
  for (int i = 0; i < bounded_attempt; ++i) {
    if (backoff >= max_delay / 2) {
      backoff = max_delay;
      break;
    }
    backoff *= 2;
  }
  backoff = std::min(backoff, max_delay);

  const int jitter_max = std::max(options_.reconnect_jitter_ms, 0);
  if (jitter_max <= 0) {
    return backoff;
  }
  static thread_local std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<int> dist(0, jitter_max);
  return backoff + dist(rng);
}

}  // namespace autobot::executor::upbit
