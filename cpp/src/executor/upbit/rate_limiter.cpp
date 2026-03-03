#include "upbit/rate_limiter.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace autobot::executor::upbit {

UpbitRateLimiter::UpbitRateLimiter(
    bool enabled,
    int ban_cooldown_sec,
    std::unordered_map<std::string, double> group_rates)
    : enabled_(enabled),
      ban_cooldown_sec_(std::max(ban_cooldown_sec, 1)),
      group_rates_(std::move(group_rates)) {}

void UpbitRateLimiter::Acquire(const std::string& group) {
  if (!enabled_) {
    return;
  }

  const std::string group_name = group.empty() ? "default" : group;
  while (true) {
    double wait_seconds = 0.0;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const double now = MonotonicSeconds();
      GroupState& state = GetOrCreateState(group_name, now);
      Refill(&state, now);

      const double cooldown_wait =
          std::max(std::max(global_cooldown_until_ - now, state.cooldown_until - now), 0.0);
      if (cooldown_wait <= 0.0 && state.tokens >= 1.0) {
        state.tokens -= 1.0;
        return;
      }

      if (cooldown_wait > 0.0) {
        wait_seconds = cooldown_wait;
      } else {
        wait_seconds = (1.0 - state.tokens) / state.rate_per_sec;
      }
    }
    std::this_thread::sleep_for(std::chrono::duration<double>(std::max(wait_seconds, 0.01)));
  }
}

void UpbitRateLimiter::ObserveRemainingReq(const RemainingReqInfo& info) {
  if (!enabled_ || !info.valid) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const double now = MonotonicSeconds();
  GroupState& state = GetOrCreateState(info.group, now);
  Refill(&state, now);
  state.last_remaining_sec = info.sec;

  if (info.sec <= 0) {
    state.tokens = 0.0;
    state.cooldown_until = std::max(state.cooldown_until, now + 1.0);
    return;
  }
  state.tokens = std::min(state.tokens, static_cast<double>(info.sec));
}

double UpbitRateLimiter::Register429(const std::string& group, int attempt) {
  const double delay_sec = std::max(1.0, std::min(8.0, static_cast<double>(1 << std::max(attempt - 1, 0))));
  if (!enabled_) {
    return delay_sec;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const double now = MonotonicSeconds();
  GroupState& state = GetOrCreateState(group.empty() ? "default" : group, now);
  state.tokens = 0.0;
  state.cooldown_until = std::max(state.cooldown_until, now + delay_sec);
  return delay_sec;
}

double UpbitRateLimiter::Register418(const std::string& group, int cooldown_sec) {
  const double delay_sec = static_cast<double>(std::max(cooldown_sec, ban_cooldown_sec_));
  if (!enabled_) {
    return delay_sec;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const double now = MonotonicSeconds();
  const double until = now + delay_sec;
  GroupState& state = GetOrCreateState(group.empty() ? "default" : group, now);
  state.tokens = 0.0;
  state.cooldown_until = std::max(state.cooldown_until, until);
  global_cooldown_until_ = std::max(global_cooldown_until_, until);
  return delay_sec;
}

double UpbitRateLimiter::MonotonicSeconds() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::duration<double>>(now).count();
}

UpbitRateLimiter::GroupState& UpbitRateLimiter::GetOrCreateState(const std::string& group, double now) {
  const auto found = states_.find(group);
  if (found != states_.end()) {
    return found->second;
  }

  GroupState created;
  created.rate_per_sec = ResolveRate(group);
  created.capacity = std::max(created.rate_per_sec, 1.0);
  created.tokens = created.capacity;
  created.last_refill_at = now;
  auto [it, _] = states_.emplace(group, created);
  return it->second;
}

double UpbitRateLimiter::ResolveRate(const std::string& group) const {
  const auto it = group_rates_.find(group);
  if (it != group_rates_.end()) {
    return std::max(it->second, 0.1);
  }
  const auto fallback = group_rates_.find("default");
  if (fallback != group_rates_.end()) {
    return std::max(fallback->second, 0.1);
  }
  return 10.0;
}

void UpbitRateLimiter::Refill(GroupState* state, double now) {
  if (state == nullptr) {
    return;
  }
  const double elapsed = std::max(now - state->last_refill_at, 0.0);
  if (elapsed <= 0.0) {
    return;
  }
  state->tokens = std::min(state->capacity, state->tokens + elapsed * state->rate_per_sec);
  state->last_refill_at = now;
}

}  // namespace autobot::executor::upbit
