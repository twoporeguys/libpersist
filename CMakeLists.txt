cmake_minimum_required(VERSION 2.8)
project(libpersist)

find_package (PkgConfig REQUIRED)
pkg_check_modules (GLIB REQUIRED glib-2.0>=2.40)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)
pkg_check_modules(LIBRPC REQUIRED librpc)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "-fblocks -Wall -Wextra -Wno-unused-parameter ${PKGCONFIG_C_FLAGS}")
set(CMAKE_CXX_FLAGS "-fblocks -Wall -Wextra -Wno-unused-parameter ${PKGCONFIG_C_FLAGS}")
include_directories(include)
include_directories(/usr/local/include)
include_directories (${GLIB_INCLUDE_DIRS})
link_directories(${GLIB_LIBRARY_DIRS})
include_directories(${LIBRPC_INCLUDE_DIRS})
link_directories(${LIBRPC_LIBRARY_DIRS})
link_directories(${YAML_LIBRARY_DIRS})

set(HEADERS
        include/persist.h)

set(CORE_FILES
        src/persist.c
        src/utils.c
        src/internal.h
        src/linker_set.h)

set(DRIVER_FILES
        src/drivers/sqlite.c)

if(BUNDLED_BLOCKS_RUNTIME)
    set(CORE_FILES ${CORE_FILES}
            contrib/BlocksRuntime/data.c
            contrib/BlocksRuntime/runtime.c)
endif()

add_library(libpersist SHARED
        ${HEADERS}
        ${CORE_FILES}
        ${DRIVER_FILES})

set_target_properties(libpersist PROPERTIES PREFIX "")
set_target_properties(libpersist PROPERTIES SOVERSION 1)
set_target_properties(libpersist PROPERTIES MACOSX_RPATH ON)
target_link_libraries(libpersist ${GLIB_LIBRARIES})
target_link_libraries(libpersist ${LIBRPC_LIBRARIES})
