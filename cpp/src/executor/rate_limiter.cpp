#include "rate_limiter.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace autobot::executor {

namespace {
double MonotonicSeconds() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(now).count();
}
}  // namespace

RateLimiter::RateLimiter(double rate_per_sec)
    : rate_per_sec_(std::max(rate_per_sec, 0.1)),
      capacity_(std::max(rate_per_sec, 1.0)),
      tokens_(std::max(rate_per_sec, 1.0)),
      last_refill_sec_(MonotonicSeconds()) {}

void RateLimiter::Acquire() {
  while (true) {
    double wait_seconds = 0.0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const double now = MonotonicSeconds();
      const double elapsed = std::max(now - last_refill_sec_, 0.0);
      if (elapsed > 0.0) {
        tokens_ = std::min(capacity_, tokens_ + elapsed * rate_per_sec_);
        last_refill_sec_ = now;
      }
      if (tokens_ >= 1.0) {
        tokens_ -= 1.0;
        return;
      }
      wait_seconds = (1.0 - tokens_) / rate_per_sec_;
    }
    std::this_thread::sleep_for(std::chrono::duration<double>(std::max(wait_seconds, 0.01)));
  }
}

}  // namespace autobot::executor
