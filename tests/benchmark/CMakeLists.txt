add_executable(persist-benchmark benchmark.c)
target_link_libraries(persist-benchmark libpersist)
install(TARGETS persist-benchmark DESTINATION bin)
