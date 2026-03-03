#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "upbit/remaining_req.h"

namespace autobot::executor::upbit {

class UpbitRateLimiter {
 public:
  explicit UpbitRateLimiter(
      bool enabled,
      int ban_cooldown_sec,
      std::unordered_map<std::string, double> group_rates);

  void Acquire(const std::string& group);
  void ObserveRemainingReq(const RemainingReqInfo& info);
  void ObserveMissingRemainingReq(const std::string& group);
  double Register429(const std::string& group, int attempt);
  double Register418(const std::string& group, int cooldown_sec);

 private:
  struct GroupState {
    double rate_per_sec = 10.0;
    double capacity = 10.0;
    double tokens = 10.0;
    double last_refill_at = 0.0;
    double cooldown_until = 0.0;
    double conservative_until = 0.0;
    int last_remaining_sec = -1;
    int consecutive_429 = 0;
    int missing_remaining_headers = 0;
  };

  static double MonotonicSeconds();
  GroupState& GetOrCreateState(const std::string& group, double now);
  double ResolveRate(const std::string& group) const;
  static void Refill(GroupState* state, double now);

  bool enabled_;
  int ban_cooldown_sec_;
  std::unordered_map<std::string, double> group_rates_;
  std::unordered_map<std::string, GroupState> states_;
  double global_cooldown_until_ = 0.0;
  mutable std::mutex mutex_;
};

}  // namespace autobot::executor::upbit
