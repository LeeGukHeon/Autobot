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
  upbit/auth_jwt.cpp
  upbit/querystring.cpp
  upbit/remaining_req.cpp
  upbit/rate_limiter.cpp
  upbit/http_client.cpp
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
