add_executable(persisttool persisttool.c)
target_link_libraries(persisttool libpersist)

if (not APPLE)
    target_link_libraries(persisttool BlocksRuntime)
endif()
