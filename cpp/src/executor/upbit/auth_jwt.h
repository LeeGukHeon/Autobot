#pragma once

#include <string>

namespace autobot::executor::upbit {

class UpbitJwtSigner {
 public:
  UpbitJwtSigner(std::string access_key, std::string secret_key);

  std::string BuildAuthorizationHeader(const std::string& query_string) const;

  static std::string HashQueryString(const std::string& query_string);

 private:
  std::string BuildToken(const std::string& query_string) const;
  static std::string Base64UrlEncode(const unsigned char* data, std::size_t size);
  static std::string BuildNonce();

  std::string access_key_;
  std::string secret_key_;
};

}  // namespace autobot::executor::upbit
