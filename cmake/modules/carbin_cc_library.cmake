#
# Copyright 2023 The Carbin Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


include(CMakeParseArguments)
include(carbin_config_cxx_opts)
include(carbin_install_dirs)
include(carbin_print_list)
include(carbin_print)
include(carbin_variable)

################################################################################
# Create a Library.
#
# Example usage:
#
# carbin_cc_library(  NAME myLibrary
#                  NAMESPACE myNamespace
#                  SOURCES
#                       myLib.cpp
#                       myLib_functions.cpp
#                  DEFINITIONS
#                     USE_DOUBLE_PRECISION=1
#                  PUBLIC_INCLUDE_PATHS
#                     ${CMAKE_SOURCE_DIR}/mylib/include
#                  PRIVATE_INCLUDE_PATHS
#                     ${CMAKE_SOURCE_DIR}/include
#                  PRIVATE_LINKED_TARGETS
#                     Threads::Threads
#                  PUBLIC_LINKED_TARGETS
#                     Threads::Threads
#                  LINKED_TARGETS
#                     Threads::Threads
# )
#
# The above example creates an alias target, myNamespace::myLibrary which can be
# linked to by other tar gets.
# PUBLIC_DEFINITIONS -  preprocessor defines which are inherated by targets which
#                       link to this library
#
#
# PUBLIC_INCLUDE_PATHS - include paths which are public, therefore inherted by
#                        targest which link to this library.
#
# PRIVATE_INCLUDE_PATHS - private include paths which are only visible by MyLibrary
#
# LINKED_TARGETS        - targets to link to.
################################################################################
function(carbin_cc_library)
    set(options
            PUBLIC
            SHARED
            OBJECT
            )
    set(args NAME
            NAMESPACE
            )

    set(list_args
            DEPS
            SOURCES
            HEADERS
            DEFINITIONS
            INCLUDES
            COPTS
            )

    cmake_parse_arguments(
            PARSE_ARGV 0
            CARBIN_CC_LIB
            "${options}"
            "${args}"
            "${list_args}"
    )

    if ("${CARBIN_CC_LIB_NAME}" STREQUAL "")
        get_filename_component(CARBIN_CC_LIB_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)
        string(REPLACE " " "_" CARBIN_CC_LIB_NAME ${CARBIN_CC_LIB_NAME})
        carbin_print(" Library, NAME argument not provided. Using folder name:  ${CARBIN_CC_LIB_NAME}")
    endif ()

    if ("${CARBIN_CC_LIB_NAMESPACE}" STREQUAL "")
        set(CARBIN_CC_LIB_NAMESPACE ${CARBIN_CC_LIB_NAME})
        message(" Library, NAMESPACE argument not provided. Using target alias:  ${CARBIN_CC_LIB_NAME}::${CARBIN_CC_LIB_NAME}")
    endif ()

    set(CARBIN_CC_LIB_TARGETS "${CARBIN_CC_LIB_NAME}-static")

    if ("${CARBIN_CC_LIB_SOURCES}" STREQUAL "")
        set(CARBIN_CC_LIB_IS_INTERFACE true)
    else ()
        set(CARBIN_CC_LIB_IS_INTERFACE false)
    endif ()

    if (BUILD_SHARED_LIBRARY)
        set(CARBIN_BUILD_TYPE "SHARED")
    else ()
        set(CARBIN_BUILD_TYPE "STATIC")
    endif ()


    carbin_raw("-----------------------------------")
    carbin_print_label("Create Library" "${CARBIN_CC_LIB_NAMESPACE}::${CARBIN_CC_LIB_NAME}")
    carbin_raw("-----------------------------------")
    if (VERBOSE_CARBIN_BUILD)
        carbin_print_list_label("Sources" CARBIN_CC_LIB_SOURCES)
        carbin_print_list_label("DEPS" CARBIN_CC_LIB_DEPS)
        carbin_print_list_label("COPTS" CARBIN_CC_LIB_COPTS)
        carbin_print_list_label("DEFINITIONS" CARBIN_CC_LIB_DEFINITIONS)
        if (CARBIN_CC_LIB_PUBLIC)
            carbin_print_label("Public" "true")
        else ()
            carbin_print_label("Public" "false")
        endif ()
        carbin_print_label("Libaray type" ${CARBIN_BUILD_TYPE})
        carbin_print_label("Interface" ${CARBIN_CC_LIB_IS_INTERFACE})
        carbin_raw("-----------------------------------")
    endif ()
    if (NOT CARBIN_CC_LIB_IS_INTERFACE)
        if (NOT CARBIN_CC_LIB_SHARED AND NOT CARBIN_CC_LIB_OBJECT)
            add_library(${CARBIN_CC_LIB_NAME}-static STATIC ${CARBIN_CC_LIB_SOURCES} ${CARBIN_CC_LIB_HEADERS})
            target_compile_options(${CARBIN_CC_LIB_NAME}-static PRIVATE ${CARBIN_CC_LIB_COPTS})
            target_link_libraries(${CARBIN_CC_LIB_NAME}-static PRIVATE ${CARBIN_CC_LIB_DEPS})

            target_include_directories(${CARBIN_CC_LIB_NAME}-static ${CARBIN_INTERNAL_INCLUDE_WARNING_GUARD}
                    PUBLIC
                    ${CARBIN_CC_LIB_INCLUDES}
                    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
                    "$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>"
                    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
                    )

            target_compile_definitions(${CARBIN_CC_LIB_NAME}-static PUBLIC ${CARBIN_CC_LIB_DEFINITIONS})
            set_property(TARGET ${CARBIN_CC_LIB_NAME}-static PROPERTY POSITION_INDEPENDENT_CODE 1)
            set_target_properties(${CARBIN_CC_LIB_NAME}-static PROPERTIES OUTPUT_NAME ${CARBIN_CC_LIB_NAME} CLEAN_DIRECT_OUTPUT 1)
            add_library(${CARBIN_CC_LIB_NAMESPACE}::${CARBIN_CC_LIB_NAME} ALIAS ${CARBIN_CC_LIB_NAME}-static)
        elseif (CARBIN_CC_LIB_OBJECT)
            add_library(${CARBIN_CC_LIB_NAME}-object OBJECT ${CARBIN_CC_LIB_SOURCES} ${CARBIN_CC_LIB_HEADERS})
            target_compile_options(${CARBIN_CC_LIB_NAME}-object PUBLIC ${CARBIN_CC_LIB_COPTS})
            target_link_libraries(${CARBIN_CC_LIB_NAME}-object PUBLIC ${CARBIN_CC_LIB_DEPS})

            target_include_directories(${CARBIN_CC_LIB_NAME}-object ${CARBIN_INTERNAL_INCLUDE_WARNING_GUARD}
                    PUBLIC
                    ${CARBIN_CC_LIB_INCLUDES}
                    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
                    "$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>"
                    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
                    )

            target_compile_definitions(${CARBIN_CC_LIB_NAME}-object  PUBLIC ${CARBIN_CC_LIB_DEFINITIONS})
            set_property(TARGET ${CARBIN_CC_LIB_NAME}-object PROPERTY POSITION_INDEPENDENT_CODE 1)
            add_library(${CARBIN_CC_LIB_NAMESPACE}::${CARBIN_CC_LIB_NAME}-object ALIAS ${CARBIN_CC_LIB_NAME}-object)
        else (CARBIN_CC_LIB_SHARED)
            add_library(${CARBIN_CC_LIB_NAME}-shared SHARED ${CARBIN_CC_LIB_SOURCES} ${CARBIN_CC_LIB_HEADERS})
            target_compile_options(${CARBIN_CC_LIB_NAME}-shared PUBLIC ${CARBIN_CC_LIB_COPTS})
            target_link_libraries(${CARBIN_CC_LIB_NAME}-shared PUBLIC ${CARBIN_CC_LIB_DEPS})

            target_include_directories(${CARBIN_CC_LIB_NAME}-shared ${CARBIN_INTERNAL_INCLUDE_WARNING_GUARD}
                    PUBLIC
                    ${CARBIN_CC_LIB_INCLUDES}
                    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
                    "$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>"
                    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
                    )

            target_compile_definitions(${CARBIN_CC_LIB_NAME}-shared  PUBLIC ${CARBIN_CC_LIB_DEFINITIONS})
            set_property(TARGET ${CARBIN_CC_LIB_NAME}-shared PROPERTY POSITION_INDEPENDENT_CODE 1)
            set_target_properties(${CARBIN_CC_LIB_NAME}-shared PROPERTIES OUTPUT_NAME ${CARBIN_CC_LIB_NAME} CLEAN_DIRECT_OUTPUT 1)
            add_library(${CARBIN_CC_LIB_NAMESPACE}::${CARBIN_CC_LIB_NAME}-shared ALIAS ${CARBIN_CC_LIB_NAME}-shared)
        endif ()

    else ()
        add_library(${CARBIN_CC_LIB_NAME} INTERFACE)
        add_library(${CARBIN_CC_LIB_NAMESPACE}::${CARBIN_CC_LIB_NAME} ALIAS ${CARBIN_CC_LIB_NAME})

        target_compile_options(${CARBIN_CC_LIB_NAME}_OBJ PRIVATE ${CARBIN_CC_LIB_COPTS})

        target_link_libraries(${CARBIN_CC_LIB_NAME}_OBJ PRIVATE ${CARBIN_CC_LIB_DEPS})

        target_include_directories(${CARBIN_CC_LIB_NAME} ${CARBIN_INTERNAL_INCLUDE_WARNING_GUARD}
                PUBLIC
                "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>"
                "$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>"
                "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
                )

        target_compile_definitions(${CARBIN_CC_LIB_NAME} INTERFACE ${CARBIN_CC_LIB_DEFINES})

    endif ()

    if (CARBIN_CC_LIB_PUBLIC)
        if (CARBIN_CC_LIB_SHARED)
            install(TARGETS ${CARBIN_CC_LIB_NAME}-shared
                    EXPORT ${PROJECT_NAME}Targets
                    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
                    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
                    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
                    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
                    )
        endif ()
        if (NOT CARBIN_CC_LIB_SHARED AND NOT CARBIN_CC_LIB_OBJECT)
            install(TARGETS ${CARBIN_CC_LIB_NAME}-static
                    EXPORT ${PROJECT_NAME}Targets
                    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
                    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
                    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
                    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
                    )
        endif ()
    endif ()

    foreach (arg IN LISTS CARBIN_CC_LIB_UNPARSED_ARGUMENTS)
        message(WARNING "Unparsed argument: ${arg}")
    endforeach ()

endfunction()

