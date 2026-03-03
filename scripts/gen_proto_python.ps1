param(
    [string]$ProtoPath = "proto/autobot.proto",
    [string]$ProtoInclude = "proto",
    [string]$OutDir = "autobot/execution"
)

python -m grpc_tools.protoc -I $ProtoInclude --python_out=$OutDir --grpc_python_out=$OutDir $ProtoPath

$grpcStubPath = Join-Path $OutDir "autobot_pb2_grpc.py"
if (Test-Path $grpcStubPath) {
    (Get-Content $grpcStubPath -Raw).Replace("import autobot_pb2 as autobot__pb2", "from . import autobot_pb2 as autobot__pb2") |
        Set-Content $grpcStubPath -Encoding UTF8
}
