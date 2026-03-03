#include "upbit/upbit_private.h"

#include <algorithm>
#include <cctype>

namespace autobot::executor::upbit {

namespace {

std::string Upper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  return value;
}

std::string Lower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

}  // namespace

UpbitPrivateClient::UpbitPrivateClient(UpbitHttpClient* http_client) : http_client_(http_client) {}

HttpResponse UpbitPrivateClient::CreateOrder(const OrderCreateRequest& request, bool test_mode) {
  HttpRequest req;
  req.method = "POST";
  req.endpoint = test_mode ? "/v1/orders/test" : "/v1/orders";
  req.auth = true;
  req.rate_limit_group = test_mode ? "order-test" : "order";
  req.has_json_body = true;
  req.json_body = nlohmann::json::object();

  std::vector<QueryParam> body_pairs;
  const std::string market = Upper(Trim(request.market));
  const std::string side = Lower(Trim(request.side));
  const std::string ord_type = Lower(Trim(request.ord_type));
  if (!market.empty()) {
    req.json_body["market"] = market;
    body_pairs.emplace_back("market", market);
  }
  if (!side.empty()) {
    req.json_body["side"] = side;
    body_pairs.emplace_back("side", side);
  }
  if (!ord_type.empty()) {
    req.json_body["ord_type"] = ord_type;
    body_pairs.emplace_back("ord_type", ord_type);
  }
  const std::string price = Trim(request.price);
  if (!price.empty()) {
    req.json_body["price"] = price;
    body_pairs.emplace_back("price", price);
  }
  const std::string volume = Trim(request.volume);
  if (!volume.empty()) {
    req.json_body["volume"] = volume;
    body_pairs.emplace_back("volume", volume);
  }
  const std::string tif = Lower(Trim(request.time_in_force));
  if (!tif.empty()) {
    req.json_body["time_in_force"] = tif;
    body_pairs.emplace_back("time_in_force", tif);
  }
  const std::string identifier = Trim(request.identifier);
  if (!identifier.empty()) {
    req.json_body["identifier"] = identifier;
    body_pairs.emplace_back("identifier", identifier);
  }
  req.auth_query_params = body_pairs;
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::CancelOrder(const std::string& uuid, const std::string& identifier) {
  HttpRequest req;
  req.method = "DELETE";
  req.endpoint = "/v1/order";
  req.auth = true;
  req.rate_limit_group = "order";
  const std::string uuid_value = Trim(uuid);
  const std::string identifier_value = Trim(identifier);
  if (!uuid_value.empty()) {
    req.params.emplace_back("uuid", uuid_value);
  }
  if (!identifier_value.empty()) {
    req.params.emplace_back("identifier", identifier_value);
  }
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::GetOrder(const std::string& uuid, const std::string& identifier) {
  HttpRequest req;
  req.method = "GET";
  req.endpoint = "/v1/order";
  req.auth = true;
  req.rate_limit_group = "default";
  const std::string uuid_value = Trim(uuid);
  const std::string identifier_value = Trim(identifier);
  if (!uuid_value.empty()) {
    req.params.emplace_back("uuid", uuid_value);
  }
  if (!identifier_value.empty()) {
    req.params.emplace_back("identifier", identifier_value);
  }
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::OpenOrders(const std::string& market, const std::vector<std::string>& states) {
  HttpRequest req;
  req.method = "GET";
  req.endpoint = "/v1/orders/open";
  req.auth = true;
  req.rate_limit_group = "default";
  const std::string market_value = Upper(Trim(market));
  if (!market_value.empty()) {
    req.params.emplace_back("market", market_value);
  }
  if (states.empty()) {
    req.params.emplace_back("states[]", "wait");
    req.params.emplace_back("states[]", "watch");
  } else {
    for (const std::string& state : states) {
      const std::string normalized = Lower(Trim(state));
      if (!normalized.empty()) {
        req.params.emplace_back("states[]", normalized);
      }
    }
  }
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::CancelOrdersByKeys(
    const std::vector<std::string>& uuids, const std::vector<std::string>& identifiers) {
  HttpRequest req;
  req.method = "DELETE";
  req.endpoint = "/v1/orders/uuids";
  req.auth = true;
  req.rate_limit_group = "order-cancel-all";
  for (const std::string& uuid : uuids) {
    const std::string value = Trim(uuid);
    if (!value.empty()) {
      req.params.emplace_back("uuids[]", value);
    }
  }
  for (const std::string& identifier : identifiers) {
    const std::string value = Trim(identifier);
    if (!value.empty()) {
      req.params.emplace_back("identifiers[]", value);
    }
  }
  return http_client_->RequestJson(req);
}

}  // namespace autobot::executor::upbit
