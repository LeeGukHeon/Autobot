#pragma once

#include <mutex>

namespace autobot::executor {

class RateLimiter {
 public:
  explicit RateLimiter(double rate_per_sec);
  void Acquire();

 private:
  double rate_per_sec_;
  double capacity_;
  double tokens_;
  double last_refill_sec_;
  std::mutex mutex_;
};

}  // namespace autobot::executor
