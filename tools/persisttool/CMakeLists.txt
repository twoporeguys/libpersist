add_executable(persisttool persisttool.c)
target_link_libraries(persisttool libpersist)
install(TARGETS persisttool DESTINATION bin)
