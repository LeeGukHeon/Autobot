#include "state/executor_state_store.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>

#ifdef _WIN32
#include <io.h>
#include <process.h>
#else
#include <unistd.h>
#endif

#include <nlohmann/json.hpp>

namespace autobot::executor::state {

namespace {

constexpr int kSchemaVersion = 2;
constexpr auto kStaleLockThreshold = std::chrono::minutes(10);
constexpr char kLockOwnerMetaFileName[] = "owner.json";
const nlohmann::json kNullJson = nullptr;

std::int64_t NowEpochMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int CurrentPid() {
#ifdef _WIN32
  return _getpid();
#else
  return static_cast<int>(getpid());
#endif
}

bool IsForceUnlockEnabled() {
  const char* raw = std::getenv("AUTOBOT_EXECUTOR_FORCE_UNLOCK");
  if (raw == nullptr) {
    return false;
  }
  std::string value(raw);
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  return value == "YES" || value == "TRUE" || value == "1";
}

bool WriteLockOwnerMetadata(const std::filesystem::path& lock_path) {
  nlohmann::json payload = {
      {"owner_pid", CurrentPid()},
      {"created_at_ms", NowEpochMs()},
  };
  const std::filesystem::path metadata_path = lock_path / kLockOwnerMetaFileName;
  std::ofstream out(metadata_path, std::ios::binary | std::ios::trunc);
  if (!out.good()) {
    return false;
  }
  out << payload.dump(2);
  out.flush();
  return out.good();
}

std::optional<std::int64_t> ReadLockCreatedAtMs(const std::filesystem::path& lock_path) {
  const std::filesystem::path metadata_path = lock_path / kLockOwnerMetaFileName;
  std::ifstream in(metadata_path, std::ios::binary);
  if (!in.good()) {
    return std::nullopt;
  }
  const nlohmann::json payload = nlohmann::json::parse(in, nullptr, false);
  if (!payload.is_object()) {
    return std::nullopt;
  }
  const auto it = payload.find("created_at_ms");
  if (it == payload.end()) {
    return std::nullopt;
  }
  if (it->is_number_integer() || it->is_number_unsigned()) {
    return it->get<std::int64_t>();
  }
  if (it->is_string()) {
    try {
      return std::stoll(it->get<std::string>());
    } catch (...) {
      return std::nullopt;
    }
  }
  return std::nullopt;
}

bool IsFilesystemEntryStaleByMtime(const std::filesystem::path& lock_path) {
  std::error_code ec;
  const auto modified_at = std::filesystem::last_write_time(lock_path, ec);
  if (ec) {
    return false;
  }
  const auto age = decltype(modified_at)::clock::now() - modified_at;
  return age >= kStaleLockThreshold;
}

bool IsStaleLock(const std::filesystem::path& lock_path) {
  const auto created_at_ms = ReadLockCreatedAtMs(lock_path);
  if (created_at_ms.has_value() && *created_at_ms > 0) {
    const auto now_ms = NowEpochMs();
    if (now_ms > *created_at_ms) {
      return (now_ms - *created_at_ms) >=
             std::chrono::duration_cast<std::chrono::milliseconds>(kStaleLockThreshold).count();
    }
    return false;
  }
  return IsFilesystemEntryStaleByMtime(lock_path);
}

bool TryForceUnlockStaleDirectory(const std::filesystem::path& lock_path) {
  if (!IsForceUnlockEnabled() || !IsStaleLock(lock_path)) {
    return false;
  }
  std::error_code ec;
  std::filesystem::remove_all(lock_path, ec);
  return !ec;
}

class ScopedDirLock {
 public:
  explicit ScopedDirLock(std::filesystem::path lock_path) : lock_path_(std::move(lock_path)) {}

  bool Acquire(std::chrono::milliseconds timeout) {
    const auto start = std::chrono::steady_clock::now();
    while (true) {
      std::error_code ec;
      if (std::filesystem::create_directory(lock_path_, ec)) {
        if (!WriteLockOwnerMetadata(lock_path_)) {
          std::error_code ignored;
          std::filesystem::remove_all(lock_path_, ignored);
          return false;
        }
        acquired_ = true;
        return true;
      }
      if (ec && ec.value() != 0 && ec != std::errc::file_exists) {
        return false;
      }
      if (TryForceUnlockStaleDirectory(lock_path_)) {
        continue;
      }
      if (std::chrono::steady_clock::now() - start >= timeout) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }

  ~ScopedDirLock() {
    if (!acquired_) {
      return;
    }
    std::error_code ignored;
    std::filesystem::remove_all(lock_path_, ignored);
  }

 private:
  std::filesystem::path lock_path_;
  bool acquired_ = false;
};

std::int64_t ParseInt64(const nlohmann::json& value) {
  if (value.is_number_integer() || value.is_number_unsigned()) {
    return value.get<std::int64_t>();
  }
  if (value.is_string()) {
    try {
      return std::stoll(value.get<std::string>());
    } catch (...) {
      return 0;
    }
  }
  return 0;
}

int ParseInt(const nlohmann::json& value, int fallback) {
  if (value.is_number_integer() || value.is_number_unsigned()) {
    return value.get<int>();
  }
  if (value.is_string()) {
    try {
      return std::stoi(value.get<std::string>());
    } catch (...) {
      return fallback;
    }
  }
  return fallback;
}

std::string ParseString(const nlohmann::json& value) {
  if (value.is_string()) {
    return value.get<std::string>();
  }
  if (value.is_number_integer()) {
    return std::to_string(value.get<long long>());
  }
  if (value.is_number_unsigned()) {
    return std::to_string(value.get<unsigned long long>());
  }
  if (value.is_number_float()) {
    return std::to_string(value.get<double>());
  }
  return "";
}

bool LoadPayloadFile(
    const std::filesystem::path& path,
    std::unordered_map<std::string, IdentifierStateRecord>* out) {
  if (out == nullptr) {
    return false;
  }
  std::ifstream in(path, std::ios::binary);
  if (!in.good()) {
    return false;
  }
  nlohmann::json payload = nlohmann::json::parse(in, nullptr, false);
  if (!payload.is_object()) {
    return false;
  }
  if (!payload.contains("schema_version")) {
    return false;
  }
  if (ParseInt(payload["schema_version"], 0) <= 0) {
    return false;
  }
  const auto records_it = payload.find("records");
  if (records_it == payload.end() || !records_it->is_object()) {
    return false;
  }

  out->clear();
  for (auto it = records_it->begin(); it != records_it->end(); ++it) {
    if (!it.value().is_object()) {
      continue;
    }
    auto field_or_null = [&](const char* key) -> const nlohmann::json& {
      const auto found = it.value().find(key);
      if (found == it.value().end()) {
        return kNullJson;
      }
      return *found;
    };
    IdentifierStateRecord record;
    record.identifier = it.key();
    record.intent_id = ParseString(field_or_null("intent_id"));
    record.mode = ParseString(field_or_null("mode"));
    record.status = ParseString(field_or_null("status"));
    record.upbit_uuid = ParseString(field_or_null("upbit_uuid"));
    record.prev_identifier = ParseString(field_or_null("prev_identifier"));
    record.prev_upbit_uuid = ParseString(field_or_null("prev_upbit_uuid"));
    record.root_identifier = ParseString(field_or_null("root_identifier"));
    record.root_upbit_uuid = ParseString(field_or_null("root_upbit_uuid"));
    record.chain_status = ParseString(field_or_null("chain_status"));
    record.replace_attempt = ParseInt(field_or_null("replace_attempt"), 0);
    record.last_replace_ts_ms = ParseInt64(field_or_null("last_replace_ts_ms"));
    record.created_at_ms = ParseInt64(field_or_null("created_at_ms"));
    record.updated_at_ms = ParseInt64(field_or_null("updated_at_ms"));
    record.last_http_status = ParseInt(field_or_null("last_http_status"), 0);
    record.last_error_name = ParseString(field_or_null("last_error_name"));
    record.last_remaining_req_group = ParseString(field_or_null("last_remaining_req_group"));
    record.last_remaining_req_sec = ParseInt(field_or_null("last_remaining_req_sec"), -1);
    if (!record.identifier.empty()) {
      (*out)[record.identifier] = std::move(record);
    }
  }
  return true;
}

nlohmann::json SerializePayload(const std::unordered_map<std::string, IdentifierStateRecord>& records) {
  nlohmann::json payload = nlohmann::json::object();
  payload["schema_version"] = kSchemaVersion;
  payload["records"] = nlohmann::json::object();
  for (const auto& [identifier, record] : records) {
    payload["records"][identifier] = {
        {"identifier", record.identifier},
        {"intent_id", record.intent_id},
        {"mode", record.mode},
        {"status", record.status},
        {"upbit_uuid", record.upbit_uuid},
        {"prev_identifier", record.prev_identifier},
        {"prev_upbit_uuid", record.prev_upbit_uuid},
        {"root_identifier", record.root_identifier},
        {"root_upbit_uuid", record.root_upbit_uuid},
        {"chain_status", record.chain_status},
        {"replace_attempt", record.replace_attempt},
        {"last_replace_ts_ms", record.last_replace_ts_ms},
        {"created_at_ms", record.created_at_ms},
        {"updated_at_ms", record.updated_at_ms},
        {"last_http_status", record.last_http_status},
        {"last_error_name", record.last_error_name},
        {"last_remaining_req_group", record.last_remaining_req_group},
        {"last_remaining_req_sec", record.last_remaining_req_sec},
    };
  }
  return payload;
}

bool WriteFileWithSync(const std::filesystem::path& path, const std::string& content) {
  FILE* fp = std::fopen(path.string().c_str(), "wb");
  if (fp == nullptr) {
    return false;
  }

  const std::size_t expected = content.size();
  const std::size_t written = std::fwrite(content.data(), 1, expected, fp);
  if (written != expected) {
    std::fclose(fp);
    return false;
  }
  std::fflush(fp);

#ifdef _WIN32
  _commit(_fileno(fp));
#else
  fsync(fileno(fp));
#endif

  return std::fclose(fp) == 0;
}

}  // namespace

ExecutorStateStore::ExecutorStateStore(std::string file_path) : file_path_(std::move(file_path)) {}

bool ExecutorStateStore::Load() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (file_path_.empty()) {
    records_.clear();
    return false;
  }

  const std::filesystem::path path(file_path_);
  ScopedDirLock lock(path.string() + ".lock");
  if (!lock.Acquire(std::chrono::milliseconds(3000))) {
    return false;
  }

  std::unordered_map<std::string, IdentifierStateRecord> loaded;
  if (LoadPayloadFile(path, &loaded)) {
    records_ = std::move(loaded);
    return true;
  }

  const std::filesystem::path backup(path.string() + ".bak");
  if (LoadPayloadFile(backup, &loaded)) {
    records_ = std::move(loaded);
    return true;
  }
  records_.clear();
  return false;
}

bool ExecutorStateStore::Save() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return SaveLocked();
}

std::optional<IdentifierStateRecord> ExecutorStateStore::Find(const std::string& identifier) const {
  if (identifier.empty()) {
    return std::nullopt;
  }
  std::lock_guard<std::mutex> guard(mutex_);
  const auto found = records_.find(identifier);
  if (found == records_.end()) {
    return std::nullopt;
  }
  return found->second;
}

std::optional<IdentifierStateRecord> ExecutorStateStore::FindByUpbitUuid(const std::string& upbit_uuid) const {
  if (upbit_uuid.empty()) {
    return std::nullopt;
  }
  std::lock_guard<std::mutex> guard(mutex_);
  for (const auto& [_, record] : records_) {
    if (record.upbit_uuid == upbit_uuid) {
      return record;
    }
  }
  return std::nullopt;
}

void ExecutorStateStore::Upsert(const IdentifierStateRecord& record) {
  if (record.identifier.empty()) {
    return;
  }
  std::lock_guard<std::mutex> guard(mutex_);
  records_[record.identifier] = record;
  SaveLocked();
}

std::unordered_map<std::string, IdentifierStateRecord> ExecutorStateStore::Snapshot() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return records_;
}

bool ExecutorStateStore::SaveLocked() const {
  if (file_path_.empty()) {
    return false;
  }

  const std::filesystem::path path(file_path_);
  const std::filesystem::path parent = path.parent_path();
  std::error_code ec;
  if (!parent.empty()) {
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return false;
    }
  }

  ScopedDirLock lock(path.string() + ".lock");
  if (!lock.Acquire(std::chrono::milliseconds(3000))) {
    return false;
  }

  const std::filesystem::path backup(path.string() + ".bak");
  if (std::filesystem::exists(path, ec) && !ec) {
    std::filesystem::copy_file(path, backup, std::filesystem::copy_options::overwrite_existing, ec);
    ec.clear();
  }

  const std::filesystem::path tmp(path.string() + ".tmp");
  const std::string serialized = SerializePayload(records_).dump(2);
  if (!WriteFileWithSync(tmp, serialized)) {
    return false;
  }

  std::filesystem::remove(path, ec);
  ec.clear();
  std::filesystem::rename(tmp, path, ec);
  if (ec) {
    ec.clear();
    std::filesystem::copy_file(tmp, path, std::filesystem::copy_options::overwrite_existing, ec);
    std::filesystem::remove(tmp, ec);
    if (ec) {
      return false;
    }
  }
  return true;
}

}  // namespace autobot::executor::state
