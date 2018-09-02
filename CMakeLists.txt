cmake_minimum_required(VERSION 2.8)
include(GNUInstallDirs)
project(libpersist)

find_package (PkgConfig REQUIRED)
pkg_check_modules(GLIB REQUIRED glib-2.0>=2.40)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)
pkg_check_modules(LIBRPC REQUIRED librpc)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "-fblocks -Wall -Wextra -Wno-unused-parameter ${PKGCONFIG_C_FLAGS}")
set(CMAKE_CXX_FLAGS "-fblocks -Wall -Wextra -Wno-unused-parameter ${PKGCONFIG_C_FLAGS}")
set(PYTHON_VERSION python3 CACHE STRING "Target Python version for py-persist")

option(BUILD_PYTHON "Build and install Python extension" ON)

include_directories(include)
include_directories(/usr/local/include)
include_directories (${GLIB_INCLUDE_DIRS})
link_directories(${GLIB_LIBRARY_DIRS})
include_directories (${SQLITE3_INCLUDE_DIRS})
link_directories(${SQLITE3_LIBRARY_DIRS})
include_directories(${LIBRPC_INCLUDE_DIRS})
link_directories(${LIBRPC_LIBRARY_DIRS})

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

function(python_install SETUP_PY)
    if(EXISTS "$ENV{DESTDIR}/${CMAKE_INSTALL_PREFIX}/lib/${PYTHON_VERSION}/dist-packages")
        install(CODE "
        execute_process(
            COMMAND ${PYTHON} ${SETUP_PY} install
                --record python_manifest.txt
                --root=/\$ENV{DESTDIR}
                --install-lib=${CMAKE_INSTALL_PREFIX}/lib/${PYTHON_VERSION}/dist-packages
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        )")
    elseif(DEFINED "$ENV{CUSTOM_PREFIX}")
        install(CODE "
        execute_process(
            COMMAND ${PYTHON} ${SETUP_PY} install
                --record python_manifest.txt
                --root=/\$ENV{DESTDIR}
                --prefix=${CMAKE_INSTALL_PREFIX}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        )")
    else()
        install(CODE "
        execute_process(
            COMMAND ${PYTHON} ${SETUP_PY} install
                --record python_manifest.txt
                --root=/\$ENV{DESTDIR}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        )")
    endif()
endfunction()

if(BUILD_PYTHON)
    add_subdirectory(bindings/python)
endif()

set_target_properties(libpersist PROPERTIES PREFIX "")
set_target_properties(libpersist PROPERTIES SOVERSION 1)
set_target_properties(libpersist PROPERTIES MACOSX_RPATH ON)
target_link_libraries(libpersist ${GLIB_LIBRARIES})
target_link_libraries(libpersist ${LIBRPC_LIBRARIES})
target_link_libraries(libpersist ${SQLITE3_LIBRARIES})

if(NOT APPLE)
	target_link_libraries(libpersist BlocksRuntime)
endif()

configure_file(libpersist.pc.in ${CMAKE_CURRENT_BINARY_DIR}/libpersist.pc @ONLY)
install(TARGETS libpersist DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libpersist.pc DESTINATION ${CMAKE_INSTALL_PREFIX}/share/pkgconfig)

add_subdirectory(tools/persisttool)
