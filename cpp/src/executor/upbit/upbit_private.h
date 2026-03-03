#pragma once

#include <string>
#include <vector>

#include "upbit/http_client.h"

namespace autobot::executor::upbit {

struct OrderCreateRequest {
  std::string market;
  std::string side;
  std::string ord_type = "limit";
  std::string price;
  std::string volume;
  std::string time_in_force;
  std::string identifier;
};

class UpbitPrivateClient {
 public:
  explicit UpbitPrivateClient(UpbitHttpClient* http_client);

  HttpResponse CreateOrder(const OrderCreateRequest& request, bool test_mode);
  HttpResponse CancelOrder(const std::string& uuid, const std::string& identifier);
  HttpResponse GetOrder(const std::string& uuid, const std::string& identifier);
  HttpResponse OpenOrders(const std::string& market, const std::vector<std::string>& states);
  HttpResponse CancelOrdersByKeys(
      const std::vector<std::string>& uuids, const std::vector<std::string>& identifiers);

 private:
  UpbitHttpClient* http_client_;
};

}  // namespace autobot::executor::upbit
