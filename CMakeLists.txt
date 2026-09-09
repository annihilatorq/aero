cmake_minimum_required(VERSION 3.25)

list(APPEND CMAKE_MODULE_PATH 
	"${CMAKE_CURRENT_SOURCE_DIR}/cmake"
	"${CMAKE_CURRENT_SOURCE_DIR}/cmake/scripts"
)

include(Prelude)

project(aero_cpp_library LANGUAGES C CXX)

include(AeroOptions)
include(AeroUseAsio)
include(AeroUseTLS)

if (PROJECT_IS_TOP_LEVEL)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
	set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)
endif()

add_library(aero INTERFACE)
add_library(aero::aero ALIAS aero)

# Add an existing dependency target or fetch it from git
if(NOT AERO_TLS_BACKEND STREQUAL "none")
	aero_use_tls_library(aero)
else()
	message(STATUS "[AERO] Building without TLS support")
endif()
aero_use_asio(aero)

target_include_directories(aero INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(aero INTERFACE cxx_std_23)

if(AERO_BUILD_EXAMPLES)
	add_subdirectory(examples)
endif()

if(AERO_BUILD_TESTS)
  include(CTest)
  add_subdirectory(tests)
endif()
