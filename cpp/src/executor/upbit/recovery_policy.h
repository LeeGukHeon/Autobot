#pragma once

#include <string>

#include "upbit/http_client.h"

namespace autobot::executor::upbit {

enum class RecoveryAction {
  kSuccess = 0,
  kFail = 1,
  kRecoverByGetIdentifier = 2,
};

struct RecoveryDecision {
  RecoveryAction action = RecoveryAction::kFail;
  std::string reason;
  bool operator_intervention_required = false;
};

RecoveryDecision DecideCreateOrderRecovery(const HttpResponse& response);

}  // namespace autobot::executor::upbit

