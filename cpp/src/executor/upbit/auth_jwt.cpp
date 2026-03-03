#include "upbit/auth_jwt.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <array>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

namespace autobot::executor::upbit {

namespace {

std::string ToHex(const unsigned char* data, std::size_t size) {
  std::ostringstream oss;
  oss << std::hex << std::setfill('0');
  for (std::size_t i = 0; i < size; ++i) {
    oss << std::setw(2) << static_cast<int>(data[i]);
  }
  return oss.str();
}

}  // namespace

UpbitJwtSigner::UpbitJwtSigner(std::string access_key, std::string secret_key)
    : access_key_(std::move(access_key)), secret_key_(std::move(secret_key)) {
  if (access_key_.empty()) {
    throw std::runtime_error("UPBIT access key is empty");
  }
  if (secret_key_.empty()) {
    throw std::runtime_error("UPBIT secret key is empty");
  }
}

std::string UpbitJwtSigner::BuildAuthorizationHeader(const std::string& query_string) const {
  return "Bearer " + BuildToken(query_string);
}

std::string UpbitJwtSigner::HashQueryString(const std::string& query_string) {
  unsigned char digest[SHA512_DIGEST_LENGTH];
  SHA512(reinterpret_cast<const unsigned char*>(query_string.data()), query_string.size(), digest);
  return ToHex(digest, SHA512_DIGEST_LENGTH);
}

std::string UpbitJwtSigner::BuildToken(const std::string& query_string) const {
  nlohmann::json payload = {
      {"access_key", access_key_},
      {"nonce", BuildNonce()},
  };
  if (!query_string.empty()) {
    payload["query_hash"] = HashQueryString(query_string);
    payload["query_hash_alg"] = "SHA512";
  }

  static const std::string header_json = R"({"alg":"HS512","typ":"JWT"})";
  const std::string payload_json = payload.dump();

  const std::string encoded_header =
      Base64UrlEncode(reinterpret_cast<const unsigned char*>(header_json.data()), header_json.size());
  const std::string encoded_payload =
      Base64UrlEncode(reinterpret_cast<const unsigned char*>(payload_json.data()), payload_json.size());

  const std::string signing_input = encoded_header + "." + encoded_payload;
  unsigned char signature[EVP_MAX_MD_SIZE];
  unsigned int signature_len = 0;
  if (HMAC(EVP_sha512(),
           secret_key_.data(),
           static_cast<int>(secret_key_.size()),
           reinterpret_cast<const unsigned char*>(signing_input.data()),
           signing_input.size(),
           signature,
           &signature_len) == nullptr) {
    throw std::runtime_error("failed to build JWT signature");
  }

  const std::string encoded_signature = Base64UrlEncode(signature, signature_len);
  return signing_input + "." + encoded_signature;
}

std::string UpbitJwtSigner::Base64UrlEncode(const unsigned char* data, std::size_t size) {
  if (data == nullptr || size == 0) {
    return "";
  }

  std::string encoded;
  encoded.resize(4 * ((size + 2) / 3));
  const int written =
      EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()), data, static_cast<int>(size));
  if (written < 0) {
    throw std::runtime_error("failed to base64 encode");
  }
  encoded.resize(static_cast<std::size_t>(written));

  for (char& ch : encoded) {
    if (ch == '+') {
      ch = '-';
    } else if (ch == '/') {
      ch = '_';
    }
  }
  while (!encoded.empty() && encoded.back() == '=') {
    encoded.pop_back();
  }
  return encoded;
}

std::string UpbitJwtSigner::BuildNonce() {
  std::array<unsigned char, 16> raw{};
  if (RAND_bytes(raw.data(), static_cast<int>(raw.size())) != 1) {
    throw std::runtime_error("failed to generate nonce bytes");
  }
  raw[6] = static_cast<unsigned char>((raw[6] & 0x0F) | 0x40);  // v4
  raw[8] = static_cast<unsigned char>((raw[8] & 0x3F) | 0x80);  // variant

  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (int i = 0; i < 16; ++i) {
    out << std::setw(2) << static_cast<int>(raw[static_cast<std::size_t>(i)]);
    if (i == 3 || i == 5 || i == 7 || i == 9) {
      out << '-';
    }
  }
  return out.str();
}

}  // namespace autobot::executor::upbit
