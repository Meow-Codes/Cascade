add_executable(bench_queue bench_queue.cpp)
target_link_libraries(bench_queue PRIVATE cascade_core)

add_executable(bench_memory_pool bench_memory_pool.cpp)
target_link_libraries(bench_memory_pool PRIVATE cascade_core)

add_executable(bench_tcp bench_tcp.cpp)
target_link_libraries(bench_tcp PRIVATE cascade_core)

add_executable(bench_storage bench_storage.cpp)
target_link_libraries(bench_storage PRIVATE cascade_core)

add_executable(bench_broker bench_broker.cpp)
target_link_libraries(bench_broker PRIVATE cascade_core)

add_executable(bench_media bench_media.cpp)
target_link_libraries(bench_media PRIVATE cascade_core)