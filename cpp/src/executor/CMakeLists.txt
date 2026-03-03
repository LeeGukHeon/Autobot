set(PROTO_FILE "${PROJECT_SOURCE_DIR}/../proto/autobot.proto")
get_filename_component(PROTO_ABS "${PROTO_FILE}" ABSOLUTE)
get_filename_component(PROTO_DIR "${PROTO_ABS}" DIRECTORY)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

set(GENERATED_PROTO_SRC "${CMAKE_CURRENT_BINARY_DIR}/autobot.pb.cc")
set(GENERATED_PROTO_HDR "${CMAKE_CURRENT_BINARY_DIR}/autobot.pb.h")
set(GENERATED_GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/autobot.grpc.pb.cc")
set(GENERATED_GRPC_HDR "${CMAKE_CURRENT_BINARY_DIR}/autobot.grpc.pb.h")

add_custom_command(
  OUTPUT
    "${GENERATED_PROTO_SRC}"
    "${GENERATED_PROTO_HDR}"
    "${GENERATED_GRPC_SRC}"
    "${GENERATED_GRPC_HDR}"
  COMMAND "$<TARGET_FILE:protobuf::protoc>"
  ARGS
    "--proto_path=${PROTO_DIR}"
    "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}"
    "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}"
    "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    "${PROTO_ABS}"
  DEPENDS
    "${PROTO_ABS}"
  COMMENT "Generating C++ protobuf/gRPC sources for executor"
  VERBATIM
)

add_library(autobot_executor_proto STATIC
  "${GENERATED_PROTO_SRC}"
  "${GENERATED_GRPC_SRC}"
)
target_include_directories(autobot_executor_proto PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(autobot_executor_proto
  PUBLIC
    gRPC::grpc++
    protobuf::libprotobuf
)

add_executable(autobot_executor
  main.cpp
  grpc_server.cpp
  upbit_rest.cpp
  order_manager.cpp
  state/executor_state_store.cpp
  upbit/auth_jwt.cpp
  upbit/number_string.cpp
  upbit/querystring.cpp
  upbit/recovery_policy.cpp
  upbit/request_builder.cpp
  upbit/remaining_req.cpp
  upbit/rate_limiter.cpp
  upbit/tif_policy.cpp
  upbit/http_client.cpp
  upbit/ws_keepalive_scheduler.cpp
  upbit/ws_private_client.cpp
  upbit/ws_private_parsers.cpp
  upbit/upbit_private.cpp
)

target_include_directories(autobot_executor PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(autobot_executor
  PRIVATE
    autobot_executor_proto
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
)

if(WIN32)
  target_link_libraries(autobot_executor PRIVATE winhttp)
endif()

add_executable(autobot_executor_unit_tests
  tests/executor_unit_tests.cpp
  upbit_rest.cpp
  state/executor_state_store.cpp
  upbit/auth_jwt.cpp
  upbit/number_string.cpp
  upbit/recovery_policy.cpp
  upbit/request_builder.cpp
  upbit/remaining_req.cpp
  upbit/rate_limiter.cpp
  upbit/tif_policy.cpp
  upbit/http_client.cpp
  upbit/ws_private_parsers.cpp
  upbit/upbit_private.cpp
)
target_include_directories(autobot_executor_unit_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(autobot_executor_unit_tests
  PRIVATE
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
)
if(WIN32)
  target_link_libraries(autobot_executor_unit_tests PRIVATE winhttp)
endif()

add_executable(autobot_executor_ws_private_parsers_tests
  tests/ws_private_parsers_tests.cpp
  upbit/number_string.cpp
  upbit/ws_private_parsers.cpp
)
target_include_directories(autobot_executor_ws_private_parsers_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(autobot_executor_ws_private_parsers_tests
  PRIVATE
    nlohmann_json::nlohmann_json
)

add_executable(autobot_executor_ws_keepalive_scheduler_tests
  tests/ws_keepalive_scheduler_tests.cpp
  upbit/ws_keepalive_scheduler.cpp
)
target_include_directories(autobot_executor_ws_keepalive_scheduler_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(autobot_executor_ws_keepalive_scheduler_tests
  PRIVATE
    nlohmann_json::nlohmann_json
)

add_executable(autobot_executor_fault_drills_tests
  tests/fault_drills_tests.cpp
  tests/fault_injection_transport.cpp
  order_manager.cpp
  upbit_rest.cpp
  state/executor_state_store.cpp
  upbit/auth_jwt.cpp
  upbit/number_string.cpp
  upbit/recovery_policy.cpp
  upbit/request_builder.cpp
  upbit/remaining_req.cpp
  upbit/rate_limiter.cpp
  upbit/tif_policy.cpp
  upbit/http_client.cpp
  upbit/ws_keepalive_scheduler.cpp
  upbit/ws_private_client.cpp
  upbit/ws_private_parsers.cpp
  upbit/upbit_private.cpp
)
target_include_directories(autobot_executor_fault_drills_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(autobot_executor_fault_drills_tests
  PRIVATE
    OpenSSL::Crypto
    nlohmann_json::nlohmann_json
)
if(WIN32)
  target_link_libraries(autobot_executor_fault_drills_tests PRIVATE winhttp)
endif()

enable_testing()
add_test(
  NAME autobot_executor_unit_tests
  COMMAND $<TARGET_FILE:autobot_executor_unit_tests>
)
add_test(
  NAME autobot_executor_ws_private_parsers_tests
  COMMAND $<TARGET_FILE:autobot_executor_ws_private_parsers_tests>
)
add_test(
  NAME autobot_executor_ws_keepalive_scheduler_tests
  COMMAND $<TARGET_FILE:autobot_executor_ws_keepalive_scheduler_tests>
)
add_test(
  NAME autobot_executor_fault_drills_tests
  COMMAND $<TARGET_FILE:autobot_executor_fault_drills_tests>
)
