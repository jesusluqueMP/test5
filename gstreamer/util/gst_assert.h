#pragma once

#include <common/except.h>

namespace caspar { namespace gstreamer {

struct gstreamer_error_t : virtual caspar_exception
{
};

using gstreamer_error_info = boost::error_info<struct tag_gstreamer_error_info, std::string>;

}} // namespace caspar::gstreamer

#define GST_CHECK(call, msg) \
    if (!(call)) { \
        CASPAR_THROW_EXCEPTION(caspar::gstreamer::gstreamer_error_t() \
                              << boost::errinfo_api_function(#call) \
                              << caspar::gstreamer::gstreamer_error_info(msg)); \
    }

#define GST_ERROR_CHECK(result, msg) \
    if ((result) != GST_FLOW_OK && (result) != GST_FLOW_EOS) { \
        CASPAR_THROW_EXCEPTION(caspar::gstreamer::gstreamer_error_t() \
                              << caspar::gstreamer::gstreamer_error_info(msg) \
                              << boost::errinfo_api_function(gst_flow_get_name(result))); \
    }