#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <nlohmann/json.hpp>

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
  double price = 0.0;
  double volume = 0.0;
  double executed_volume = 0.0;
  std::string remaining_req_group = "default";
  int remaining_req_sec = -1;
};

class UpbitRestClient {
 public:
  explicit UpbitRestClient(bool order_test_mode);

  bool IsOrderTestMode() const;
  UpbitSubmitResult SubmitLimitOrder(const UpbitSubmitRequest& request);
  UpbitCancelResult CancelOrder(const UpbitCancelRequest& request);
  UpbitOrderResult GetOrder(const std::string& upbit_uuid, const std::string& identifier);

 private:
  static double ParseJsonNumber(const nlohmann::json& payload, const char* key);
  static std::string ParseJsonString(const nlohmann::json& payload, const char* key);
  static std::string BuildMockUuid(const std::string& identifier);
  static std::string FormatNumber(double value);

  void LoadState();
  void SaveStateLocked() const;
  void UpsertIdentifierMapping(const std::string& identifier, const std::string& upbit_uuid);
  std::string ResolveMappedUuid(const std::string& identifier) const;
  static std::string ResolveStateFilePath();

  bool order_test_mode_;
  std::string state_file_path_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::string> identifier_to_uuid_;
  std::unique_ptr<upbit::UpbitHttpClient> http_client_;
  std::unique_ptr<upbit::UpbitPrivateClient> private_client_;
};

}  // namespace autobot::executor
