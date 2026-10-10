add_subdirectory(proto)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBCURL REQUIRED IMPORTED_TARGET libcurl)

add_library(inference_core
  gateway/worker_registry.cpp
  gateway/scheduler.cpp
  gateway/gateway_service.cpp
  worker/mock_generation_backend.cpp
  worker/llama_cpp_backend.cpp
  worker/gpu_memory_probe.cpp
  worker/inference_worker_service.cpp)
target_include_directories(inference_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(inference_core PUBLIC inference_proto spark_push_common
  PkgConfig::LIBCURL ${CMAKE_DL_LIBS})

add_executable(inference_gateway gateway/main.cpp)
target_link_libraries(inference_gateway PRIVATE inference_core)
add_executable(inference_worker worker/main.cpp)
target_link_libraries(inference_worker PRIVATE inference_core)
add_executable(inference_cli client/main.cpp)
target_link_libraries(inference_cli PRIVATE inference_proto)

if(BUILD_TESTING)
  add_library(inference_fake_cuda SHARED tests/fake_cuda_driver.cpp)
  set_target_properties(inference_fake_cuda PROPERTIES OUTPUT_NAME cuda SOVERSION 1
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test-driver)
  add_library(inference_fake_nvml SHARED tests/fake_nvml.cpp)
  set_target_properties(inference_fake_nvml PROPERTIES OUTPUT_NAME nvidia-ml SOVERSION 1
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/test-driver)
  add_executable(gpu_memory_probe_test tests/gpu_memory_probe_test.cpp)
  target_link_libraries(gpu_memory_probe_test PRIVATE inference_core)
  foreach(probe_case IN ITEMS success init sample nvml)
    add_test(NAME inference_cuda_probe_${probe_case} COMMAND gpu_memory_probe_test)
    set_tests_properties(inference_cuda_probe_${probe_case} PROPERTIES
      ENVIRONMENT "LD_LIBRARY_PATH=$<TARGET_FILE_DIR:inference_fake_cuda>")
    if(NOT probe_case STREQUAL "success")
      set_property(TEST inference_cuda_probe_${probe_case} APPEND PROPERTY
        ENVIRONMENT "SPARK_TEST_CUDA_FAIL=${probe_case}")
    endif()
  endforeach()
  find_package(Python3 3.10 COMPONENTS Interpreter REQUIRED)
  add_executable(inference_tests tests/inference_tests.cpp
    ${PROJECT_SOURCE_DIR}/hermes_bridge/grpc_inference_client.cpp)
  target_include_directories(inference_tests PRIVATE ${PROJECT_SOURCE_DIR}/hermes_bridge)
  target_link_libraries(inference_tests PRIVATE inference_core)
  add_test(NAME inference_tests COMMAND inference_tests)
  set_tests_properties(inference_tests PROPERTIES TIMEOUT 30)
  add_test(NAME inference_llamacpp_adapter_test
    COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/llama_cpp_adapter_test.py
      $<TARGET_FILE:inference_gateway>
      $<TARGET_FILE:inference_worker>
      $<TARGET_FILE:inference_cli>)
  set_tests_properties(inference_llamacpp_adapter_test PROPERTIES TIMEOUT 30)
  add_test(NAME inference_vllm_adapter_contract_test
    COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/llama_cpp_adapter_test.py
      $<TARGET_FILE:inference_gateway>
      $<TARGET_FILE:inference_worker>
      $<TARGET_FILE:inference_cli> vllm)
  set_tests_properties(inference_vllm_adapter_contract_test PROPERTIES TIMEOUT 30)
endif()
