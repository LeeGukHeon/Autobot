#include "grpc_server.h"

#include <chrono>
#include <iostream>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

namespace autobot::executor {

namespace {
std::int64_t NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}
}  // namespace

ExecutionServiceImpl::ExecutionServiceImpl(OrderManager* order_manager) : order_manager_(order_manager) {}

grpc::Status ExecutionServiceImpl::SubmitIntent(
    grpc::ServerContext* context,
    const autobot::execution::v1::OrderIntent* request,
    autobot::execution::v1::SubmitResult* response) {
  (void)context;
  ManagedIntent intent;
  intent.intent_id = request->intent_id();
  intent.identifier = request->identifier();
  intent.market = request->market();
  intent.side = SideToString(request->side());
  intent.ord_type = OrdTypeToString(request->ord_type());
  intent.price = request->price();
  intent.volume = request->volume();
  intent.tif = TifToString(request->tif());
  intent.ts_ms = request->ts_ms();
  intent.meta_json = request->meta_json();
  ManagedResult result = order_manager_->SubmitIntent(intent);

  response->set_accepted(result.accepted);
  response->set_reason(result.reason);
  response->set_upbit_uuid(result.upbit_uuid);
  response->set_identifier(result.identifier);
  response->set_intent_id(result.intent_id);
  return grpc::Status::OK;
}

grpc::Status ExecutionServiceImpl::Cancel(
    grpc::ServerContext* context,
    const autobot::execution::v1::CancelRequest* request,
    autobot::execution::v1::SubmitResult* response) {
  (void)context;
  ManagedResult result = order_manager_->Cancel(request->upbit_uuid(), request->identifier());
  response->set_accepted(result.accepted);
  response->set_reason(result.reason);
  response->set_upbit_uuid(result.upbit_uuid);
  response->set_identifier(result.identifier);
  response->set_intent_id(result.intent_id);
  return grpc::Status::OK;
}

grpc::Status ExecutionServiceImpl::ReplaceOrder(
    grpc::ServerContext* context,
    const autobot::execution::v1::ReplaceRequest* request,
    autobot::execution::v1::ReplaceResult* response) {
  (void)context;
  ManagedReplaceRequest replace_request;
  replace_request.intent_id = request->intent_id();
  replace_request.prev_order_uuid = request->prev_order_uuid();
  replace_request.prev_order_identifier = request->prev_order_identifier();
  replace_request.new_identifier = request->new_identifier();
  replace_request.new_price_str = request->new_price_str();
  replace_request.new_volume_str = request->new_volume_str();
  replace_request.new_time_in_force = request->new_time_in_force();

  const ManagedReplaceResult result = order_manager_->ReplaceOrder(replace_request);
  response->set_accepted(result.accepted);
  response->set_reason(result.reason);
  response->set_cancelled_order_uuid(result.cancelled_order_uuid);
  response->set_new_order_uuid(result.new_order_uuid);
  response->set_new_identifier(result.new_identifier);
  return grpc::Status::OK;
}

grpc::Status ExecutionServiceImpl::StreamEvents(
    grpc::ServerContext* context,
    const autobot::execution::v1::HealthRequest* request,
    grpc::ServerWriter<autobot::execution::v1::Event>* writer) {
  (void)request;
  while (!context->IsCancelled()) {
    ManagedEvent event;
    if (!order_manager_->PopEvent(&event, std::chrono::milliseconds(500))) {
      continue;
    }
    autobot::execution::v1::Event response;
    response.set_event_type(ToProtoEventType(event.event_type));
    response.set_ts_ms(event.ts_ms);
    response.set_payload_json(event.payload.dump());
    if (!writer->Write(response)) {
      break;
    }
  }
  return grpc::Status::OK;
}

grpc::Status ExecutionServiceImpl::GetSnapshot(
    grpc::ServerContext* context,
    const autobot::execution::v1::HealthRequest* request,
    autobot::execution::v1::Event* response) {
  (void)context;
  (void)request;
  response->set_event_type(autobot::execution::v1::HEALTH);
  response->set_ts_ms(NowMs());
  response->set_payload_json(order_manager_->Snapshot().dump());
  return grpc::Status::OK;
}

grpc::Status ExecutionServiceImpl::Health(
    grpc::ServerContext* context,
    const autobot::execution::v1::HealthRequest* request,
    autobot::execution::v1::HealthResponse* response) {
  (void)context;
  (void)request;
  response->set_ok(true);
  response->set_message("executor_alive");
  response->set_ts_ms(NowMs());
  return grpc::Status::OK;
}

autobot::execution::v1::EventType ExecutionServiceImpl::ToProtoEventType(const std::string& event_type) {
  if (event_type == "ORDER_UPDATE") {
    return autobot::execution::v1::ORDER_UPDATE;
  }
  if (event_type == "FILL") {
    return autobot::execution::v1::FILL;
  }
  if (event_type == "ASSET") {
    return autobot::execution::v1::ASSET;
  }
  if (event_type == "ERROR") {
    return static_cast<autobot::execution::v1::EventType>(5);
  }
  return autobot::execution::v1::HEALTH;
}

std::string ExecutionServiceImpl::SideToString(autobot::execution::v1::Side side) {
  if (side == autobot::execution::v1::ASK) {
    return "ask";
  }
  return "bid";
}

std::string ExecutionServiceImpl::OrdTypeToString(autobot::execution::v1::OrdType ord_type) {
  if (ord_type == autobot::execution::v1::LIMIT) {
    return "limit";
  }
  return "limit";
}

std::string ExecutionServiceImpl::TifToString(autobot::execution::v1::TimeInForce tif) {
  if (tif == autobot::execution::v1::IOC) {
    return "ioc";
  }
  if (tif == autobot::execution::v1::FOK) {
    return "fok";
  }
  return "gtc";
}

ExecutorGrpcServer::ExecutorGrpcServer(Options options)
    : options_(std::move(options)),
      rest_client_(options_.order_test_mode),
      order_manager_(&rest_client_),
      service_(&order_manager_) {}

int ExecutorGrpcServer::Run() {
  grpc::ServerBuilder builder;
  const std::string bind_addr = options_.host + ":" + std::to_string(options_.port);
  builder.AddListeningPort(bind_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (server == nullptr) {
    std::cerr << "[executor] failed to start gRPC server: " << bind_addr << std::endl;
    return 2;
  }

  std::cout << "[executor] listening on " << bind_addr
            << " order_test_mode=" << (options_.order_test_mode ? "true" : "false") << std::endl;
  server->Wait();
  return 0;
}

}  // namespace autobot::executor
