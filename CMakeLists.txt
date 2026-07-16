cmake_minimum_required(VERSION 3.16)

project(DistributedKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(DKV_BUILD_TESTS "Build DistributedKV tests" ON)
option(DKV_BUILD_BENCHMARKS "Build DistributedKV benchmarks" ON)

find_package(Threads REQUIRED)

add_library(distributed_kv_storage
    src/storage/kv_store.cpp
    src/storage/wal.cpp
)

target_include_directories(distributed_kv_storage
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(distributed_kv_storage
    PUBLIC
        Threads::Threads
)

target_compile_options(distributed_kv_storage
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

add_library(distributed_kv_network
    src/network/connection.cpp
    src/network/peer_transport.cpp
    src/network/protocol.cpp
    src/network/raft_protocol.cpp
    src/network/server.cpp
    src/network/tcp_client.cpp
)

target_include_directories(distributed_kv_network
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(distributed_kv_network
    PUBLIC
        distributed_kv_storage
        Threads::Threads
)

target_compile_options(distributed_kv_network
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

add_library(distributed_kv_raft
    src/raft/cluster_config.cpp
    src/raft/file_raft_persistence.cpp
    src/raft/file_snapshot_store.cpp
    src/raft/kv_command.cpp
    src/raft/leader_election.cpp
    src/raft/log_replication.cpp
    src/raft/raft_kv_service.cpp
    src/raft/raft_log.cpp
    src/raft/raft_node.cpp
    src/raft/state_machine.cpp
)

target_include_directories(distributed_kv_raft
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(distributed_kv_raft
    PUBLIC
        distributed_kv_storage
)

target_compile_options(distributed_kv_raft
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

add_library(distributed_kv_runtime
    src/server/node_service.cpp
)

target_include_directories(distributed_kv_runtime
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(distributed_kv_runtime
    PUBLIC
        distributed_kv_network
        distributed_kv_raft
        Threads::Threads
)

target_compile_options(distributed_kv_runtime
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

add_executable(dkv_server
    src/server/main.cpp
)

target_link_libraries(dkv_server
    PRIVATE
        distributed_kv_network
)

target_compile_options(dkv_server
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

add_executable(dkv_node
    src/server/node_main.cpp
)

target_link_libraries(dkv_node
    PRIVATE
        distributed_kv_runtime
        stdc++fs
)

target_compile_options(dkv_node
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

add_executable(dkv_client
    src/client/main.cpp
)

add_executable(dkv_admin
    src/client/admin_main.cpp
)

target_link_libraries(dkv_admin
    PRIVATE
        distributed_kv_network
)

target_compile_options(dkv_admin
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

target_link_libraries(dkv_client
    PRIVATE
        distributed_kv_network
)

target_compile_options(dkv_client
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wshadow
)

if(DKV_BUILD_BENCHMARKS)
    add_executable(dkv_cluster_benchmark
        benchmarks/cluster_benchmark.cpp
    )

    target_link_libraries(dkv_cluster_benchmark
        PRIVATE
            distributed_kv_runtime
    )

    target_compile_options(dkv_cluster_benchmark
        PRIVATE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )
endif()

if(DKV_BUILD_TESTS)
    include(CTest)
    include(FetchContent)

    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.17.0
        GIT_SHALLOW TRUE
    )
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    add_executable(kv_store_test
        tests/kv_store_test.cpp
    )

    target_link_libraries(kv_store_test
        PRIVATE
            distributed_kv_storage
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(kv_store_test)

    add_executable(wal_test
        tests/wal_test.cpp
    )

    target_link_libraries(wal_test
        PRIVATE
            distributed_kv_storage
            GTest::gtest_main
    )

    gtest_discover_tests(wal_test)

    add_executable(raft_log_test
        tests/raft_log_test.cpp
    )

    target_link_libraries(raft_log_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(raft_log_test)

    add_executable(raft_persistence_test
        tests/raft_persistence_test.cpp
    )

    target_link_libraries(raft_persistence_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(raft_persistence_test)

    add_executable(snapshot_store_test
        tests/snapshot_store_test.cpp
    )

    target_link_libraries(snapshot_store_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(snapshot_store_test)

    add_executable(node_service_test
        tests/node_service_test.cpp
    )

    target_link_libraries(node_service_test
        PRIVATE
            distributed_kv_runtime
            GTest::gtest_main
    )

    gtest_discover_tests(node_service_test)

    add_executable(cluster_process_test
        tests/cluster_process_test.cpp
    )

    target_compile_definitions(cluster_process_test
        PRIVATE
            DKV_NODE_PATH="$<TARGET_FILE:dkv_node>"
    )

    target_link_libraries(cluster_process_test
        PRIVATE
            distributed_kv_network
            GTest::gtest_main
    )

    add_dependencies(cluster_process_test dkv_node)
    gtest_discover_tests(cluster_process_test)

    add_executable(raft_algorithms_test
        tests/raft_algorithms_test.cpp
    )

    target_link_libraries(raft_algorithms_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(raft_algorithms_test)

    add_executable(state_machine_test
        tests/state_machine_test.cpp
    )

    target_link_libraries(state_machine_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(state_machine_test)

    add_executable(raft_kv_service_test
        tests/raft_kv_service_test.cpp
    )

    target_link_libraries(raft_kv_service_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(raft_kv_service_test)

    add_executable(raft_node_test
        tests/raft_node_test.cpp
    )

    target_link_libraries(raft_node_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(raft_node_test)

    add_executable(raft_cluster_test
        tests/raft_cluster_test.cpp
    )

    target_link_libraries(raft_cluster_test
        PRIVATE
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(raft_cluster_test)

    add_executable(protocol_test
        tests/protocol_test.cpp
    )

    target_link_libraries(protocol_test
        PRIVATE
            distributed_kv_network
            GTest::gtest_main
    )

    gtest_discover_tests(protocol_test)

    add_executable(raft_protocol_test
        tests/raft_protocol_test.cpp
    )

    target_link_libraries(raft_protocol_test
        PRIVATE
            distributed_kv_network
            GTest::gtest_main
    )

    gtest_discover_tests(raft_protocol_test)

    add_executable(connection_test
        tests/connection_test.cpp
    )

    target_link_libraries(connection_test
        PRIVATE
            distributed_kv_network
            GTest::gtest_main
    )

    gtest_discover_tests(connection_test)

    add_executable(network_integration_test
        tests/network_integration_test.cpp
    )

    target_link_libraries(network_integration_test
        PRIVATE
            distributed_kv_network
            distributed_kv_raft
            GTest::gtest_main
    )

    gtest_discover_tests(network_integration_test)
endif()
