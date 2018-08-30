find_program(PYTHON "${PYTHON_VERSION}")

set(SETUP_PY ${CMAKE_SOURCE_DIR}/bindings/python/setup.py)
set(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/timestamp)
set(DEPS
        ${CMAKE_SOURCE_DIR}/bindings/python/persist.pyx
        ${CMAKE_SOURCE_DIR}/bindings/python/persist.pxd)

add_custom_command(
        OUTPUT ${OUTPUT}
        COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_SOURCE_DIR}/bindings/python
            ${CMAKE_CURRENT_BINARY_DIR}
        COMMAND ${CMAKE_COMMAND} -E env
            CMAKE_SOURCE_DIR=${CMAKE_SOURCE_DIR}
            CMAKE_PREFIX=${CMAKE_INSTALL_PREFIX}
            ${PYTHON} ${SETUP_PY} build
        COMMAND ${CMAKE_COMMAND} -E touch ${OUTPUT}
        DEPENDS ${DEPS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_custom_target(py-persist ALL DEPENDS ${OUTPUT})
python_install(${SETUP_PY})
