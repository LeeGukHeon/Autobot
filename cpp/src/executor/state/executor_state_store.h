#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace autobot::executor::state {

struct IdentifierStateRecord {
  std::string identifier;
  std::string intent_id;
  std::string mode;
  std::string status;
  std::string upbit_uuid;
  std::string prev_identifier;
  std::string prev_upbit_uuid;
  std::string root_identifier;
  std::string root_upbit_uuid;
  std::string chain_status;
  int replace_attempt = 0;
  std::int64_t last_replace_ts_ms = 0;
  std::int64_t created_at_ms = 0;
  std::int64_t updated_at_ms = 0;
  int last_http_status = 0;
  std::string last_error_name;
  std::string last_remaining_req_group;
  int last_remaining_req_sec = -1;
};

class ExecutorStateStore {
 public:
  explicit ExecutorStateStore(std::string file_path);

  bool Load();
  bool Save() const;

  std::optional<IdentifierStateRecord> Find(const std::string& identifier) const;
  std::optional<IdentifierStateRecord> FindByUpbitUuid(const std::string& upbit_uuid) const;
  void Upsert(const IdentifierStateRecord& record);
  std::unordered_map<std::string, IdentifierStateRecord> Snapshot() const;

 private:
  bool SaveLocked() const;

  std::string file_path_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, IdentifierStateRecord> records_;
};

}  // namespace autobot::executor::state
