#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>

#include "state/executor_state_store.h"
#include "upbit/http_client.h"
#include "upbit/upbit_private.h"

namespace autobot::executor {

struct UpbitSubmitRequest {
  std::string intent_id;
  std::string identifier;
  std::string market;
  std::string side;
  double price = 0.0;
  double volume = 0.0;
  std::string tif;
  std::string meta_json;
};

struct UpbitSubmitResult {
  bool accepted = false;
  std::string reason;
  std::string upbit_uuid;
  std::string identifier;
  std::string state;
  std::string remaining_req_group = "default";
  int remaining_req_sec = -1;
  bool retriable = false;
  bool recovered_by_get = false;
  bool operator_intervention_required = false;
  int http_status = 0;
  std::string error_name;
  std::string breaker_state = "none";
};

struct UpbitCancelRequest {
  std::string upbit_uuid;
  std::string identifier;
};

struct UpbitCancelResult {
  bool accepted = false;
  std::string reason;
  std::string upbit_uuid;
  std::string identifier;
  std::string state;
  std::string remaining_req_group = "default";
  int remaining_req_sec = -1;
  bool retriable = false;
  int http_status = 0;
  std::string error_name;
  std::string breaker_state = "none";
};

struct UpbitOrderResult {
  bool ok = false;
  bool found = false;
  bool retriable = false;
  std::string reason;
  std::string upbit_uuid;
  std::string identifier;
  std::string market;
  std::string side;
  std::string ord_type;
  std::string state;
  std::string price_str;
  std::string volume_str;
  std::string executed_volume_str;
  std::string remaining_volume_str;
  std::string avg_price_str;
  std::string remaining_req_group = "default";
  int remaining_req_sec = -1;
  int http_status = 0;
  std::string error_name;
  std::string breaker_state = "none";
};

struct UpbitReplaceRequest {
  std::string intent_id;
  std::string prev_order_uuid;
  std::string prev_order_identifier;
  std::string new_identifier;
  std::string new_price_str;
  std::string new_volume_str;
  std::string new_time_in_force;
};

struct UpbitReplaceResult {
  bool accepted = false;
  std::string reason;
  std::string cancelled_order_uuid;
  std::string new_order_uuid;
  std::string new_identifier;
  std::string remaining_req_group = "default";
  int remaining_req_sec = -1;
  bool retriable = false;
  int http_status = 0;
  std::string error_name;
  std::string breaker_state = "none";
};

struct UpbitAccountBalance {
  std::string currency;
  std::string balance_str;
  std::string locked_str;
  std::string avg_buy_price_str;
};

struct UpbitAccountsSnapshotResult {
  bool ok = false;
  std::string reason;
  std::vector<UpbitAccountBalance> accounts;
  std::string remaining_req_group = "default";
  int remaining_req_sec = -1;
  int http_status = 0;
  std::string error_name;
  std::string breaker_state = "none";
};

class UpbitRestClient {
 public:
  explicit UpbitRestClient(
      bool order_test_mode,
      std::unique_ptr<upbit::UpbitHttpClient> http_client_override = nullptr);

  bool IsOrderTestMode() const;
  const std::string& ModeName() const;
  bool PrivateWsEnabled() const;
  const std::string& PrivateWsUrl() const;
  std::vector<std::string> PrivateWsOrderCodes() const;
  std::string PrivateWsAuthorizationHeader() const;
  UpbitSubmitResult SubmitLimitOrder(const UpbitSubmitRequest& request);
  UpbitCancelResult CancelOrder(const UpbitCancelRequest& request);
  UpbitReplaceResult ReplaceOrder(const UpbitReplaceRequest& request);
  UpbitOrderResult GetOrder(const std::string& upbit_uuid, const std::string& identifier);
  UpbitAccountsSnapshotResult GetAccountsSnapshot();

 private:
  static std::string ParseJsonString(const nlohmann::json& payload, const char* key);
  static std::string ResolveStateFilePath();

  bool IsLiveMarketAllowed(const std::string& market) const;

  bool order_test_mode_;
  std::string mode_name_;
  state::ExecutorStateStore state_store_;
  std::unordered_set<std::string> live_allowed_markets_;
  double live_min_notional_krw_ = 0.0;
  bool private_ws_enabled_ = false;
  std::string private_ws_url_;
  std::vector<std::string> private_ws_order_codes_;
  std::optional<upbit::UpbitJwtSigner> private_ws_signer_;
  std::unique_ptr<upbit::UpbitHttpClient> http_client_;
  std::unique_ptr<upbit::UpbitPrivateClient> private_client_;
};

}  // namespace autobot::executor
