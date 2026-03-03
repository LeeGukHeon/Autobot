#include "upbit/recovery_policy.h"

namespace autobot::executor::upbit {

RecoveryDecision DecideCreateOrderRecovery(const HttpResponse& response) {
  RecoveryDecision decision;
  if (response.ok) {
    decision.action = RecoveryAction::kSuccess;
    decision.reason = "accepted";
    return decision;
  }

  if (response.status_code == 418) {
    decision.action = RecoveryAction::kFail;
    decision.reason = "rate_limited_418_global_breaker";
    decision.operator_intervention_required = true;
    return decision;
  }
  if (response.status_code == 429) {
    decision.action = RecoveryAction::kFail;
    decision.reason = "rate_limited_429_group_breaker";
    return decision;
  }

  if (response.status_code == 0) {
    decision.action = RecoveryAction::kRecoverByGetIdentifier;
    decision.reason = "network_or_timeout_unknown_result";
    return decision;
  }
  if (response.status_code >= 500) {
    decision.action = RecoveryAction::kRecoverByGetIdentifier;
    decision.reason = "server_error_unknown_result";
    return decision;
  }
  if (response.retriable && response.category != "rate_limit") {
    decision.action = RecoveryAction::kRecoverByGetIdentifier;
    decision.reason = "retriable_unknown_result";
    return decision;
  }

  decision.action = RecoveryAction::kFail;
  decision.reason = "final_error";
  return decision;
}

}  // namespace autobot::executor::upbit

