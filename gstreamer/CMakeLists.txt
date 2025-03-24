cmake_minimum_required (VERSION 3.16)
project (gstreamer)

set(SOURCES
    # Main sources
    gstreamer.cpp
    gstreamer.h
    defines.h
    StdAfx.h
    
    # Producer sources
    producer/gst_producer.cpp
    producer/gst_producer.h
    producer/gst_input.cpp
    producer/gst_input.h
    producer/gstreamer_producer.cpp
    producer/gstreamer_producer.h
    
    # Consumer sources
    consumer/gstreamer_consumer.cpp
    consumer/gstreamer_consumer.h
    
    # Utility sources
    util/gst_util.cpp
    util/gst_util.h
    util/gst_assert.h
)

# Find GStreamer packages - approach depends on platform
if(WIN32)
    # On Windows, we need to manually find GStreamer
    # User can set GSTREAMER_ROOT environment variable to point to the GStreamer installation
    if(NOT DEFINED ENV{GSTREAMER_ROOT})
        message(STATUS "GSTREAMER_ROOT environment variable not set. Using default paths.")
    else()
        set(GSTREAMER_ROOT $ENV{GSTREAMER_ROOT})
        message(STATUS "Using GStreamer from: ${GSTREAMER_ROOT}")
    endif()

    # Look for GStreamer in common installation locations
    set(GSTREAMER_SEARCH_PATHS
        ${GSTREAMER_ROOT}
        "C:/gstreamer/1.0/msvc_x86_64"
        "C:/gstreamer/1.0"
        "C:/Program Files/gstreamer/1.0"
        "C:/Program Files (x86)/gstreamer/1.0"
    )

    # Find include directories
    find_path(GSTREAMER_INCLUDE_DIR
        NAMES gst/gst.h
        PATHS ${GSTREAMER_SEARCH_PATHS}
        PATH_SUFFIXES include/gstreamer-1.0
        NO_DEFAULT_PATH
    )
    
    find_path(GLIB_INCLUDE_DIR
        NAMES glib.h
        PATHS ${GSTREAMER_SEARCH_PATHS}
        PATH_SUFFIXES include/glib-2.0
        NO_DEFAULT_PATH
    )
    
    find_path(GLIBCONFIG_INCLUDE_DIR
        NAMES glibconfig.h
        PATHS ${GSTREAMER_SEARCH_PATHS}
        PATH_SUFFIXES lib/glib-2.0/include
        NO_DEFAULT_PATH
    )

    # Find library directories
    find_path(GSTREAMER_LIBRARY_DIR
        NAMES gstreamer-1.0.lib
        PATHS ${GSTREAMER_SEARCH_PATHS}
        PATH_SUFFIXES lib
        NO_DEFAULT_PATH
    )

    # Check if we found GStreamer
    if(NOT GSTREAMER_INCLUDE_DIR OR NOT GLIB_INCLUDE_DIR OR NOT GLIBCONFIG_INCLUDE_DIR OR NOT GSTREAMER_LIBRARY_DIR)
        message(FATAL_ERROR "GStreamer not found. Please install GStreamer and set GSTREAMER_ROOT environment variable.")
    endif()

    # Set include and library paths
    set(GSTREAMER_INCLUDE_DIRS
        ${GSTREAMER_INCLUDE_DIR}
        ${GLIB_INCLUDE_DIR}
        ${GLIBCONFIG_INCLUDE_DIR}
        ${GSTREAMER_SEARCH_PATHS}/include/gstreamer-1.0
        ${GSTREAMER_SEARCH_PATHS}/include/glib-2.0
        ${GSTREAMER_SEARCH_PATHS}/lib/glib-2.0/include
    )

    # Add appropriate libraries based on debug/release configuration
    set(GSTREAMER_LIBRARIES
        ${GSTREAMER_LIBRARY_DIR}/gstreamer-1.0.lib
        ${GSTREAMER_LIBRARY_DIR}/glib-2.0.lib
        ${GSTREAMER_LIBRARY_DIR}/gobject-2.0.lib
        ${GSTREAMER_LIBRARY_DIR}/gstvideo-1.0.lib
        ${GSTREAMER_LIBRARY_DIR}/gstaudio-1.0.lib
        ${GSTREAMER_LIBRARY_DIR}/gstbase-1.0.lib
        ${GSTREAMER_LIBRARY_DIR}/gstapp-1.0.lib
    )

    message(STATUS "GStreamer include dirs: ${GSTREAMER_INCLUDE_DIRS}")
    message(STATUS "GStreamer libraries: ${GSTREAMER_LIBRARIES}")
else()
    # On Linux/Unix systems, use pkg-config
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(GSTREAMER REQUIRED gstreamer-1.0)
    pkg_check_modules(GSTREAMER_BASE REQUIRED gstreamer-base-1.0)
    pkg_check_modules(GSTREAMER_VIDEO REQUIRED gstreamer-video-1.0)
    pkg_check_modules(GSTREAMER_AUDIO REQUIRED gstreamer-audio-1.0)
    pkg_check_modules(GSTREAMER_APP REQUIRED gstreamer-app-1.0)
    
    set(GSTREAMER_INCLUDE_DIRS
        ${GSTREAMER_INCLUDE_DIRS}
        ${GSTREAMER_BASE_INCLUDE_DIRS}
        ${GSTREAMER_VIDEO_INCLUDE_DIRS}
        ${GSTREAMER_AUDIO_INCLUDE_DIRS}
        ${GSTREAMER_APP_INCLUDE_DIRS}
    )
    
    set(GSTREAMER_LIBRARIES
        ${GSTREAMER_LIBRARIES}
        ${GSTREAMER_BASE_LIBRARIES}
        ${GSTREAMER_VIDEO_LIBRARIES}
        ${GSTREAMER_AUDIO_LIBRARIES}
        ${GSTREAMER_APP_LIBRARIES}
    )
endif()

# Make sure all source files are found and verify paths
foreach(SOURCE_FILE ${SOURCES})
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${SOURCE_FILE}")
        message(WARNING "Source file not found: ${CMAKE_CURRENT_SOURCE_DIR}/${SOURCE_FILE}")
    endif()
endforeach()

casparcg_add_module_project(gstreamer
    SOURCES ${SOURCES}
    INIT_FUNCTION "gstreamer::init"
    UNINIT_FUNCTION "gstreamer::uninit"
)

target_include_directories(gstreamer PRIVATE
    ../..
    ${GSTREAMER_INCLUDE_DIRS}
)

# Using plain signature to match the style in CMakeModules/CasparCG_Util.cmake
target_link_libraries(gstreamer
    ${GSTREAMER_LIBRARIES}
)

# Copy GStreamer DLLs to output directory on Windows
# But avoid direct dependency on the casparcg target to prevent circular dependency
if(WIN32)
    # Instead of copying to $<TARGET_FILE_DIR:casparcg>, use a fixed path or the build directory
    set(GST_RUNTIME_DIR "${CMAKE_BINARY_DIR}/gstreamer-runtime")
    
    # Create a custom target to copy the files
    add_custom_target(copy_gstreamer_dlls ALL
        COMMAND ${CMAKE_COMMAND} -E make_directory "${GST_RUNTIME_DIR}"
        COMMAND ${CMAKE_COMMAND} -E echo "Copying GStreamer DLLs to ${GST_RUNTIME_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${GSTREAMER_LIBRARY_DIR}/../bin" "${GST_RUNTIME_DIR}"
        COMMENT "Copying GStreamer runtime files"
    )
    
    # Make the gstreamer module depend on this copying
    add_dependencies(gstreamer copy_gstreamer_dlls)
    
    # Add a post-build step to the main CasparCG executable (handled in the parent CMakeLists.txt)
    # This should be added to the main CasparCG CMakeLists.txt:
    # add_custom_command(TARGET casparcg POST_BUILD
    #     COMMAND ${CMAKE_COMMAND} -E copy_directory "${GST_RUNTIME_DIR}" "$<TARGET_FILE_DIR:casparcg>"
    # )
    
    # Output a message to instruct the user about the next steps
    message(STATUS "NOTE: To complete GStreamer setup, manually copy files from '${GST_RUNTIME_DIR}' to your CasparCG installation directory")
endif()

target_precompile_headers(gstreamer PRIVATE "StdAfx.h")

set_target_properties(gstreamer PROPERTIES FOLDER modules)
source_group(sources ./*)
source_group(sources\\consumer ./consumer/.*)
source_group(sources\\producer ./producer/.*)
source_group(sources\\util ./util/.*)
