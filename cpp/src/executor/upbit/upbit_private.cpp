#include "upbit/upbit_private.h"

#include <algorithm>
#include <cctype>
#include <cmath>

#include "upbit/request_builder.h"

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

HttpResponse BuildValidationError(const std::string& message) {
  HttpResponse response;
  response.ok = false;
  response.status_code = 400;
  response.category = "validation";
  response.retriable = false;
  response.error_name = "validation_error";
  response.error_message = message;
  return response;
}

HttpRequest ToHttpRequest(PreparedRequest prepared, bool auth, bool allow_retry, std::string group) {
  HttpRequest req;
  req.method = std::move(prepared.method);
  req.endpoint = std::move(prepared.path);
  req.url_query = std::move(prepared.url_query);
  req.auth_query = std::move(prepared.query_string_for_hash);
  req.body_json = std::move(prepared.body_json);
  req.headers = std::move(prepared.headers);
  req.auth = auth;
  req.allow_retry = allow_retry;
  req.rate_limit_group = std::move(group);
  return req;
}

bool IsPositiveNumberString(const std::string& raw) {
  const std::string value = Trim(raw);
  if (value.empty()) {
    return false;
  }
  try {
    const double parsed = std::stod(value);
    return std::isfinite(parsed) && parsed > 0.0;
  } catch (...) {
    return false;
  }
}

}  // namespace

UpbitPrivateClient::UpbitPrivateClient(UpbitHttpClient* http_client) : http_client_(http_client) {}

HttpResponse UpbitPrivateClient::CreateOrder(const OrderCreateRequest& request, bool test_mode) {
  OrderedParams body;
  const std::string market = Upper(Trim(request.market));
  const std::string side = Lower(Trim(request.side));
  const std::string ord_type = Lower(Trim(request.ord_type));
  const std::string price = Trim(request.price);
  const std::string volume = Trim(request.volume);
  const std::string tif = Lower(Trim(request.time_in_force));
  const std::string identifier = Trim(request.identifier);

  if (!market.empty()) {
    body.emplace_back("market", market);
  }
  if (!side.empty()) {
    body.emplace_back("side", side);
  }
  if (!ord_type.empty()) {
    body.emplace_back("ord_type", ord_type);
  }
  if (!price.empty()) {
    body.emplace_back("price", price);
  }
  if (!volume.empty()) {
    body.emplace_back("volume", volume);
  }
  if (!tif.empty()) {
    body.emplace_back("time_in_force", tif);
  }
  if (!identifier.empty()) {
    body.emplace_back("identifier", identifier);
  }

  RequestSpec spec;
  spec.method = "POST";
  spec.path = test_mode ? "/v1/orders/test" : "/v1/orders";
  spec.body_params = std::move(body);
  PreparedRequest prepared = BuildPreparedRequest(spec);
  HttpRequest req = ToHttpRequest(prepared, true, false, test_mode ? "order-test" : "order");
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::CancelOrder(const std::string& uuid, const std::string& identifier) {
  const std::string uuid_value = Trim(uuid);
  const std::string identifier_value = Trim(identifier);
  if (uuid_value.empty() && identifier_value.empty()) {
    return BuildValidationError("uuid or identifier is required");
  }

  RequestSpec spec;
  spec.method = "DELETE";
  spec.path = "/v1/order";
  if (!uuid_value.empty()) {
    spec.query_params.emplace_back("uuid", uuid_value);
  } else {
    spec.query_params.emplace_back("identifier", identifier_value);
  }

  PreparedRequest prepared = BuildPreparedRequest(spec);
  HttpRequest req = ToHttpRequest(prepared, true, true, "order");
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::CancelAndNewOrder(const CancelAndNewRequest& request) {
  const std::string prev_uuid = Trim(request.prev_order_uuid);
  const std::string prev_identifier = Trim(request.prev_order_identifier);
  const std::string new_identifier = Trim(request.new_identifier);
  const std::string new_price = Trim(request.new_price);
  const std::string new_volume = Trim(request.new_volume);
  const std::string new_tif = Lower(Trim(request.new_time_in_force));

  if (prev_uuid.empty() && prev_identifier.empty()) {
    return BuildValidationError("prev_order_uuid or prev_order_identifier is required");
  }
  if (new_identifier.empty()) {
    return BuildValidationError("new_identifier is required");
  }
  if (new_price.empty() || !IsPositiveNumberString(new_price)) {
    return BuildValidationError("new_price must be a positive number string");
  }
  if (new_volume.empty()) {
    return BuildValidationError("new_volume is required");
  }
  if (Lower(new_volume) != "remain_only" && !IsPositiveNumberString(new_volume)) {
    return BuildValidationError("new_volume must be a positive number string or remain_only");
  }

  RequestSpec spec;
  spec.method = "POST";
  spec.path = "/v1/orders/cancel_and_new";
  if (!prev_uuid.empty()) {
    spec.body_params.emplace_back("prev_order_uuid", prev_uuid);
  } else {
    spec.body_params.emplace_back("prev_order_identifier", prev_identifier);
  }
  spec.body_params.emplace_back("new_identifier", new_identifier);
  spec.body_params.emplace_back("new_ord_type", "limit");
  spec.body_params.emplace_back("new_price", new_price);
  spec.body_params.emplace_back("new_volume", new_volume);
  if (!new_tif.empty()) {
    spec.body_params.emplace_back("new_time_in_force", new_tif);
  }

  PreparedRequest prepared = BuildPreparedRequest(spec);
  HttpRequest req = ToHttpRequest(prepared, true, true, "order");
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::GetOrder(const std::string& uuid, const std::string& identifier) {
  const std::string uuid_value = Trim(uuid);
  const std::string identifier_value = Trim(identifier);
  if (uuid_value.empty() && identifier_value.empty()) {
    return BuildValidationError("uuid or identifier is required");
  }

  RequestSpec spec;
  spec.method = "GET";
  spec.path = "/v1/order";
  if (!uuid_value.empty()) {
    spec.query_params.emplace_back("uuid", uuid_value);
  } else {
    spec.query_params.emplace_back("identifier", identifier_value);
  }

  PreparedRequest prepared = BuildPreparedRequest(spec);
  HttpRequest req = ToHttpRequest(prepared, true, true, "default");
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::Accounts() {
  RequestSpec spec;
  spec.method = "GET";
  spec.path = "/v1/accounts";

  PreparedRequest prepared = BuildPreparedRequest(spec);
  HttpRequest req = ToHttpRequest(prepared, true, true, "default");
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::OpenOrders(const std::string& market, const std::vector<std::string>& states) {
  RequestSpec spec;
  spec.method = "GET";
  spec.path = "/v1/orders/open";

  const std::string market_value = Upper(Trim(market));
  if (!market_value.empty()) {
    spec.query_params.emplace_back("market", market_value);
  }
  if (states.empty()) {
    spec.query_params.emplace_back("states[]", "wait");
    spec.query_params.emplace_back("states[]", "watch");
  } else {
    for (const std::string& state : states) {
      const std::string normalized = Lower(Trim(state));
      if (!normalized.empty()) {
        spec.query_params.emplace_back("states[]", normalized);
      }
    }
  }

  PreparedRequest prepared = BuildPreparedRequest(spec);
  HttpRequest req = ToHttpRequest(prepared, true, true, "default");
  return http_client_->RequestJson(req);
}

HttpResponse UpbitPrivateClient::CancelOrdersByKeys(
    const std::vector<std::string>& uuids, const std::vector<std::string>& identifiers) {
  std::vector<std::string> trimmed_uuids;
  std::vector<std::string> trimmed_identifiers;
  trimmed_uuids.reserve(uuids.size());
  trimmed_identifiers.reserve(identifiers.size());

  for (const std::string& uuid : uuids) {
    const std::string value = Trim(uuid);
    if (!value.empty()) {
      trimmed_uuids.push_back(value);
    }
  }
  for (const std::string& identifier : identifiers) {
    const std::string value = Trim(identifier);
    if (!value.empty()) {
      trimmed_identifiers.push_back(value);
    }
  }

  if (!trimmed_uuids.empty() && !trimmed_identifiers.empty()) {
    return BuildValidationError("uuids[] and identifiers[] cannot be used together");
  }
  if (trimmed_uuids.empty() && trimmed_identifiers.empty()) {
    return BuildValidationError("uuids[] or identifiers[] is required");
  }
  if (trimmed_uuids.size() > 20 || trimmed_identifiers.size() > 20) {
    return BuildValidationError("uuids[] or identifiers[] exceeds max 20");
  }

  RequestSpec spec;
  spec.method = "DELETE";
  spec.path = "/v1/orders/uuids";
  for (const std::string& uuid : trimmed_uuids) {
    spec.query_params.emplace_back("uuids[]", uuid);
  }
  for (const std::string& identifier : trimmed_identifiers) {
    spec.query_params.emplace_back("identifiers[]", identifier);
  }

  PreparedRequest prepared = BuildPreparedRequest(spec);
  HttpRequest req = ToHttpRequest(prepared, true, true, "order-cancel-all");
  return http_client_->RequestJson(req);
}

}  // namespace autobot::executor::upbit
