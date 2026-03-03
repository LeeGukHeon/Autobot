#include "auth_jwt.h"

#include <openssl/sha.h>

#include <iomanip>
#include <sstream>
#include <utility>

namespace autobot::executor {

JwtSigner::JwtSigner(std::string access_key, std::string secret_key)
    : access_key_(std::move(access_key)), secret_key_(std::move(secret_key)) {}

std::string JwtSigner::BuildAuthorizationHeader(const std::string& query_string) const {
  // Placeholder signer for T08 MVP. This keeps interface shape identical to production.
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(query_string.data()), query_string.size(), digest);
  std::ostringstream hashed;
  for (unsigned char byte : digest) {
    hashed << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte);
  }
  return "Bearer MOCK." + access_key_ + "." + hashed.str();
}

}  // namespace autobot::executor
