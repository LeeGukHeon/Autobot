#pragma once

#include <string>

namespace autobot::executor {

class JwtSigner {
 public:
  JwtSigner(std::string access_key, std::string secret_key);
  std::string BuildAuthorizationHeader(const std::string& query_string) const;

 private:
  std::string access_key_;
  std::string secret_key_;
};

}  // namespace autobot::executor
