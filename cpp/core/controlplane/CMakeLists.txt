find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

set(PROTO_FILE ${CMAKE_SOURCE_DIR}/go/proto/cascade.proto)
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(PROTO_SRCS ${GENERATED_DIR}/cascade.pb.cc)
set(PROTO_HDRS ${GENERATED_DIR}/cascade.pb.h)
set(GRPC_SRCS  ${GENERATED_DIR}/cascade.grpc.pb.cc)
set(GRPC_HDRS  ${GENERATED_DIR}/cascade.grpc.pb.h)

find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

add_custom_command(
  OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
  COMMAND protobuf::protoc
  ARGS --cpp_out=${GENERATED_DIR}
       --grpc_out=${GENERATED_DIR}
       --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
       -I ${CMAKE_SOURCE_DIR}/go/proto
       ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating C++ gRPC stubs from cascade.proto"
)

add_library(cascade_controlplane_proto STATIC ${PROTO_SRCS} ${GRPC_SRCS})
target_include_directories(cascade_controlplane_proto PUBLIC ${GENERATED_DIR})
target_link_libraries(cascade_controlplane_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_library(cascade_controlplane_client INTERFACE)
target_sources(cascade_controlplane_client INTERFACE control_plane_client.hpp)
target_link_libraries(cascade_controlplane_client INTERFACE cascade_controlplane_proto)
target_include_directories(cascade_controlplane_client INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})