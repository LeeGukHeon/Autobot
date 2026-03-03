#pragma once

#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "autobot.grpc.pb.h"
#include "order_manager.h"
#include "upbit_rest.h"

namespace autobot::executor {

class ExecutionServiceImpl final : public autobot::execution::v1::ExecutionService::Service {
 public:
  explicit ExecutionServiceImpl(OrderManager* order_manager);

  grpc::Status SubmitIntent(
      grpc::ServerContext* context,
      const autobot::execution::v1::OrderIntent* request,
      autobot::execution::v1::SubmitResult* response) override;

  grpc::Status Cancel(
      grpc::ServerContext* context,
      const autobot::execution::v1::CancelRequest* request,
      autobot::execution::v1::SubmitResult* response) override;

  grpc::Status StreamEvents(
      grpc::ServerContext* context,
      const autobot::execution::v1::HealthRequest* request,
      grpc::ServerWriter<autobot::execution::v1::Event>* writer) override;

  grpc::Status GetSnapshot(
      grpc::ServerContext* context,
      const autobot::execution::v1::HealthRequest* request,
      autobot::execution::v1::Event* response) override;

  grpc::Status Health(
      grpc::ServerContext* context,
      const autobot::execution::v1::HealthRequest* request,
      autobot::execution::v1::HealthResponse* response) override;

 private:
  static autobot::execution::v1::EventType ToProtoEventType(const std::string& event_type);
  static std::string SideToString(autobot::execution::v1::Side side);
  static std::string OrdTypeToString(autobot::execution::v1::OrdType ord_type);
  static std::string TifToString(autobot::execution::v1::TimeInForce tif);

  OrderManager* order_manager_;
};

class ExecutorGrpcServer {
 public:
  struct Options {
    std::string host = "0.0.0.0";
    int port = 50051;
    bool order_test_mode = true;
  };

  explicit ExecutorGrpcServer(Options options);
  int Run();

 private:
  Options options_;
  UpbitRestClient rest_client_;
  OrderManager order_manager_;
  ExecutionServiceImpl service_;
};

}  // namespace autobot::executor
