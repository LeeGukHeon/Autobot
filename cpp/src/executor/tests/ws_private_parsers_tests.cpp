#include <cassert>
#include <string>
#include <vector>

#include "upbit/ws_private_parsers.h"

namespace {

void TestBuildPrivateSubscribePayload() {
  using autobot::executor::upbit::BuildPrivateSubscribePayload;

  std::string error;
  const std::string payload = BuildPrivateSubscribePayload(
      "ticket-1",
      /*subscribe_my_order=*/true,
      /*subscribe_my_asset=*/true,
      "SIMPLE_LIST",
      {"krw-btc", "KRW-ETH", "KRW-BTC"},
      &error);
  assert(error.empty());
  assert(!payload.empty());

  const nlohmann::json decoded = nlohmann::json::parse(payload, nullptr, false);
  assert(decoded.is_array());
  assert(decoded.size() == 4);
  assert(decoded[0].value("ticket", "") == "ticket-1");
  assert(decoded[1].value("type", "") == "myOrder");
  assert(decoded[1]["codes"].is_array());
  assert(decoded[1]["codes"].size() == 2);
  assert(decoded[1]["codes"][0] == "KRW-BTC");
  assert(decoded[1]["codes"][1] == "KRW-ETH");
  assert(decoded[2].value("type", "") == "myAsset");
  assert(decoded[3].value("format", "") == "SIMPLE_LIST");
}

void TestBuildPrivateSubscribePayloadDefaultFormat() {
  using autobot::executor::upbit::BuildPrivateSubscribePayload;

  std::string error;
  const std::string payload = BuildPrivateSubscribePayload(
      "ticket-default",
      /*subscribe_my_order=*/true,
      /*subscribe_my_asset=*/true,
      "DEFAULT",
      {},
      &error);
  assert(error.empty());
  assert(!payload.empty());
  const nlohmann::json decoded = nlohmann::json::parse(payload, nullptr, false);
  assert(decoded.is_array());
  assert(decoded.size() == 3);
  assert(decoded[0].value("ticket", "") == "ticket-default");
  assert(decoded[1].value("type", "") == "myOrder");
  assert(decoded[2].value("type", "") == "myAsset");
}

void TestBuildPrivateSubscribePayloadInvalidFormat() {
  using autobot::executor::upbit::BuildPrivateSubscribePayload;

  std::string error;
  const std::string payload = BuildPrivateSubscribePayload(
      "ticket-invalid",
      /*subscribe_my_order=*/true,
      /*subscribe_my_asset=*/true,
      "INVALID_FMT",
      {},
      &error);
  assert(payload.empty());
  assert(error.find("unsupported format") != std::string::npos);
}

void TestMyAssetCodesGuard() {
  using autobot::executor::upbit::BuildMyAssetSubscribeObject;

  nlohmann::json out;
  std::string error;
  const bool ok = BuildMyAssetSubscribeObject({"KRW-BTC"}, &out, &error);
  assert(!ok);
  assert(error.find("codes_not_allowed") != std::string::npos);
}

void TestParseMyOrderStates() {
  using autobot::executor::upbit::ParsePrivateWsMessage;
  using autobot::executor::upbit::WsPrivateEvent;
  using autobot::executor::upbit::WsPrivateEventKind;

  const std::vector<std::string> states = {"trade", "done", "cancel", "prevented"};
  for (const std::string& state : states) {
    const std::string raw = std::string(
                                R"({"type":"myOrder","code":"KRW-BTC","uuid":"u-1","identifier":"id-1","state":")") +
                            state + R"(","price":"100","volume":"1","executed_volume":"0.1","timestamp":1700000})";
    WsPrivateEvent parsed;
    const bool ok = ParsePrivateWsMessage(raw, &parsed);
    assert(ok);
    assert(parsed.kind == WsPrivateEventKind::kMyOrder);
    assert(parsed.order.state == state);
    assert(parsed.order.market == "KRW-BTC");
    assert(parsed.order.uuid == "u-1");
  }
}

void TestParseMyAsset() {
  using autobot::executor::upbit::ParsePrivateWsMessage;
  using autobot::executor::upbit::WsPrivateEvent;
  using autobot::executor::upbit::WsPrivateEventKind;

  const std::string raw =
      R"({"type":"myAsset","currency":"eth","balance":"0.1","locked":"0.02","avg_buy_price":"3000000","timestamp":1700001})";
  WsPrivateEvent parsed;
  const bool ok = ParsePrivateWsMessage(raw, &parsed);
  assert(ok);
  assert(parsed.kind == WsPrivateEventKind::kMyAsset);
  assert(parsed.asset.currency == "ETH");
  assert(parsed.asset.balance_str == "0.1");
  assert(parsed.asset.locked_str == "0.02");
}

void TestParseHealthUpStatus() {
  using autobot::executor::upbit::ParsePrivateWsMessage;
  using autobot::executor::upbit::WsPrivateEvent;
  using autobot::executor::upbit::WsPrivateEventKind;

  const std::string raw = R"({"status":"UP","timestamp":1700002})";
  WsPrivateEvent parsed;
  const bool ok = ParsePrivateWsMessage(raw, &parsed);
  assert(ok);
  assert(parsed.kind == WsPrivateEventKind::kHealthUp);
  assert(parsed.health_up.status == "UP");
  assert(parsed.health_up.ts_ms == 1700002);
}

}  // namespace

int main() {
  TestBuildPrivateSubscribePayload();
  TestBuildPrivateSubscribePayloadDefaultFormat();
  TestBuildPrivateSubscribePayloadInvalidFormat();
  TestMyAssetCodesGuard();
  TestParseMyOrderStates();
  TestParseMyAsset();
  TestParseHealthUpStatus();
  return 0;
}
