include(FetchContent)
include(AeroUseMagicEnum)

FetchContent_Declare(annihilatorq_ut
  GIT_REPOSITORY  https://github.com/annihilatorq/ut
  GIT_TAG         v1.0.0
  GIT_SHALLOW     TRUE
)
FetchContent_MakeAvailable(annihilatorq_ut)

add_library(aero_test_common INTERFACE)
aero_use_magic_enum(aero_test_common)

target_compile_features(aero_test_common INTERFACE cxx_std_23)
target_include_directories(aero_test_common INTERFACE ${aero_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(aero_test_common INTERFACE aero::aero ut::ut)

add_custom_target(aero_tests)

function(aero_add_unit_test target_name source_file)
  add_executable(${target_name} ${source_file})
  target_link_libraries(${target_name} PRIVATE aero_test_common)
  if(MSVC)
    target_compile_options(${target_name} PRIVATE /bigobj)
  endif()
  source_group(TREE "${CMAKE_CURRENT_LIST_DIR}" FILES "${CMAKE_CURRENT_LIST_DIR}/${source_file}")
  add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
  add_dependencies(aero_tests ${target_name})
endfunction()

function(aero_add_tls_unit_test target_name source_file)
  if(AERO_USE_TLS)
    aero_add_unit_test(${target_name} ${source_file})
  endif()
endfunction()

aero_add_unit_test(aero_error_code error_code.cpp)

add_subdirectory(util)

if(AERO_USE_TLS)
  add_subdirectory(tls)
endif()

add_subdirectory(http_primitives)
add_subdirectory(http_server)
add_subdirectory(net)
add_subdirectory(umbrella)
add_subdirectory(urls)
add_subdirectory(websocket)
