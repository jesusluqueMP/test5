#include "gst_util.h"
#include "gst_assert.h"

#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>

// Disable specific warnings for this file
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4244) // conversion from 'gsize' to 'int', possible loss of data
#endif

namespace caspar { namespace gstreamer {

GstVideoFormat pixel_format_to_gst(core::pixel_format format, common::bit_depth depth)
{
    const bool is_16bit = depth != common::bit_depth::bit8;
    
    switch (format) {
        case core::pixel_format::rgb:
            return is_16bit ? GST_VIDEO_FORMAT_RGB16 : GST_VIDEO_FORMAT_RGB;
        case core::pixel_format::bgr:
            return is_16bit ? GST_VIDEO_FORMAT_BGR16 : GST_VIDEO_FORMAT_BGR;
        case core::pixel_format::rgba:
            return GST_VIDEO_FORMAT_RGBA;
        case core::pixel_format::bgra:
            return GST_VIDEO_FORMAT_BGRA;
        case core::pixel_format::argb:
            return GST_VIDEO_FORMAT_ARGB;
        case core::pixel_format::abgr:
            return GST_VIDEO_FORMAT_ABGR;
        case core::pixel_format::ycbcr:
            // YUV formats
            if (is_16bit) {
                return GST_VIDEO_FORMAT_I420_10LE;
            } else {
                return GST_VIDEO_FORMAT_I420;
            }
        case core::pixel_format::ycbcra:
            return GST_VIDEO_FORMAT_A420;
        case core::pixel_format::luma:
        case core::pixel_format::gray:
            return is_16bit ? GST_VIDEO_FORMAT_GRAY16_LE : GST_VIDEO_FORMAT_GRAY8;
        case core::pixel_format::uyvy:
            return GST_VIDEO_FORMAT_UYVY;
        default:
            return GST_VIDEO_FORMAT_UNKNOWN;
    }
}

core::pixel_format_desc gst_format_to_caspar(GstVideoInfo* video_info)
{
    core::pixel_format format = core::pixel_format::invalid;
    core::color_space color_space = core::color_space::bt709;
    common::bit_depth depth = common::bit_depth::bit8;
    
    // Determine pixel format
    switch (GST_VIDEO_INFO_FORMAT(video_info)) {
        case GST_VIDEO_FORMAT_RGB:
            format = core::pixel_format::rgb;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_RGB16:
            format = core::pixel_format::rgb;
            depth = common::bit_depth::bit16;
            break;
        case GST_VIDEO_FORMAT_BGR:
            format = core::pixel_format::bgr;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_BGR16:
            format = core::pixel_format::bgr;
            depth = common::bit_depth::bit16;
            break;
        case GST_VIDEO_FORMAT_RGBA:
            format = core::pixel_format::rgba;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_BGRA:
            format = core::pixel_format::bgra;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_ARGB:
            format = core::pixel_format::argb;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_ABGR:
            format = core::pixel_format::abgr;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_I420:
        case GST_VIDEO_FORMAT_YV12:
            format = core::pixel_format::ycbcr;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_I420_10LE:
            format = core::pixel_format::ycbcr;
            depth = common::bit_depth::bit10;
            break;
        case GST_VIDEO_FORMAT_I420_12LE:
            format = core::pixel_format::ycbcr;
            depth = common::bit_depth::bit12;
            break;
        case GST_VIDEO_FORMAT_A420:
            format = core::pixel_format::ycbcra;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_GRAY8:
            format = core::pixel_format::gray;
            depth = common::bit_depth::bit8;
            break;
        case GST_VIDEO_FORMAT_GRAY16_LE:
            format = core::pixel_format::gray;
            depth = common::bit_depth::bit16;
            break;
        case GST_VIDEO_FORMAT_UYVY:
            format = core::pixel_format::uyvy;
            depth = common::bit_depth::bit8;
            break;
        default:
            format = core::pixel_format::invalid;
            break;
    }
    
    // Determine color space
    GstVideoColorimetry colorimetry = video_info->colorimetry;
    switch (colorimetry.matrix) {
        case GST_VIDEO_COLOR_MATRIX_BT601:
            color_space = core::color_space::bt601;
            break;
        case GST_VIDEO_COLOR_MATRIX_BT709:
            color_space = core::color_space::bt709;
            break;
        case GST_VIDEO_COLOR_MATRIX_BT2020:
            color_space = core::color_space::bt2020;
            break;
        default:
            color_space = core::color_space::bt709;
            break;
    }
    
    // Create pixel format description
    auto desc = core::pixel_format_desc(format, color_space);
    
    // Set up planes based on format
    int width = GST_VIDEO_INFO_WIDTH(video_info);
    int height = GST_VIDEO_INFO_HEIGHT(video_info);
    
    switch (format) {
        case core::pixel_format::gray:
        case core::pixel_format::luma:
            desc.planes.push_back(core::pixel_format_desc::plane(width, height, 1, depth));
            break;
        case core::pixel_format::rgb:
        case core::pixel_format::bgr:
            desc.planes.push_back(core::pixel_format_desc::plane(width, height, 3, depth));
            break;
        case core::pixel_format::rgba:
        case core::pixel_format::bgra:
        case core::pixel_format::argb:
        case core::pixel_format::abgr:
            desc.planes.push_back(core::pixel_format_desc::plane(width, height, 4, depth));
            break;
        case core::pixel_format::ycbcr:
            // Y plane
            desc.planes.push_back(core::pixel_format_desc::plane(width, height, 1, depth));
            // U plane (half resolution in both dimensions for 4:2:0)
            desc.planes.push_back(core::pixel_format_desc::plane(width/2, height/2, 1, depth));
            // V plane (half resolution in both dimensions for 4:2:0)
            desc.planes.push_back(core::pixel_format_desc::plane(width/2, height/2, 1, depth));
            break;
        case core::pixel_format::ycbcra:
            // Y plane
            desc.planes.push_back(core::pixel_format_desc::plane(width, height, 1, depth));
            // U plane (half resolution in both dimensions for 4:2:0)
            desc.planes.push_back(core::pixel_format_desc::plane(width/2, height/2, 1, depth));
            // V plane (half resolution in both dimensions for 4:2:0)
            desc.planes.push_back(core::pixel_format_desc::plane(width/2, height/2, 1, depth));
            // Alpha plane
            desc.planes.push_back(core::pixel_format_desc::plane(width, height, 1, depth));
            break;
        case core::pixel_format::uyvy:
            desc.planes.push_back(core::pixel_format_desc::plane(width/2, height, 4, depth));
            break;
        default:
            break;
    }
    
    return desc;
}

core::mutable_frame make_frame(void* tag,
                              core::frame_factory& frame_factory,
                              GstSample* sample,
                              core::color_space color_space)
{
    if (!sample) {
        return frame_factory.create_frame(tag, core::pixel_format_desc(core::pixel_format::invalid));
    }
    
    GstBuffer* buffer = gst_sample_get_buffer(sample);
    GstCaps* caps = gst_sample_get_caps(sample);
    GstVideoInfo video_info;
    
    GST_CHECK(gst_video_info_from_caps(&video_info, caps), "Failed to extract video info from caps");
    
    auto format_desc = gst_format_to_caspar((&video_info));
    auto frame = frame_factory.create_frame(tag, format_desc);
    
    GstMapInfo map;
    GST_CHECK(gst_buffer_map(buffer, &map, GST_MAP_READ), "Failed to map buffer");
    
    // Copy video data from GStreamer buffer to CasparCG frame
    switch (format_desc.format) {
        case core::pixel_format::bgra:
        case core::pixel_format::rgba:
        case core::pixel_format::argb:
        case core::pixel_format::abgr:
        case core::pixel_format::rgb:
        case core::pixel_format::bgr: {
            // For packed formats with a single plane
            auto plane = format_desc.planes[0];
            int line_size = GST_VIDEO_INFO_PLANE_STRIDE(&video_info, 0);
            
            int plane_height = static_cast<int>(plane.height);
            
            tbb::parallel_for(0, plane_height, [&](int y) {
                std::memcpy(
                    frame.image_data(0).begin() + y * plane.linesize,
                    map.data + y * line_size,
                    plane.linesize);
            });
            break;
        }
        case core::pixel_format::ycbcr: {
            // For planar YUV formats
            for (int p = 0; p < 3; ++p) {
                auto plane = format_desc.planes[p];
                int offset = GST_VIDEO_INFO_PLANE_OFFSET(&video_info, p);
                int stride = GST_VIDEO_INFO_PLANE_STRIDE(&video_info, p);
                
                int plane_height = static_cast<int>(plane.height);
                
                tbb::parallel_for(0, plane_height, [&](int y) {
                    std::memcpy(
                        frame.image_data(p).begin() + y * plane.linesize,
                        map.data + offset + y * stride,
                        plane.linesize);
                });
            }
            break;
        }
        case core::pixel_format::ycbcra: {
            // For planar YUV formats with alpha
            for (int p = 0; p < 4; ++p) {
                auto plane = format_desc.planes[p];
                int offset = GST_VIDEO_INFO_PLANE_OFFSET(&video_info, p);
                int stride = GST_VIDEO_INFO_PLANE_STRIDE(&video_info, p);
                
                int plane_height = static_cast<int>(plane.height);
                
                tbb::parallel_for(0, plane_height, [&](int y) {
                    std::memcpy(
                        frame.image_data(p).begin() + y * plane.linesize,
                        map.data + offset + y * stride,
                        plane.linesize);
                });
            }
            break;
        }
        default:
            // Handle other formats
            break;
    }
    
    gst_buffer_unmap(buffer, &map);
    
    // Handle audio if available
    // For now, we leave audio handling to be implemented later
    
    return frame;
}

GstSample* make_gst_sample(const core::const_frame& frame, const core::video_format_desc& format_desc)
{
    auto pix_desc = frame.pixel_format_desc();
    
    // Create video info
    GstVideoInfo info;
    gst_video_info_init(&info);
    
    auto gst_format = pixel_format_to_gst(pix_desc.format, pix_desc.planes[0].depth);
    if (gst_format == GST_VIDEO_FORMAT_UNKNOWN) {
        CASPAR_LOG(warning) << "Unsupported pixel format for GStreamer: " << static_cast<int>(pix_desc.format);
        return nullptr;
    }
    
    gst_video_info_set_format(&info, gst_format, format_desc.width, format_desc.height);
    
    // Create buffer
    GstBuffer* buffer = gst_buffer_new_allocate(nullptr, info.size, nullptr);
    if (!buffer) {
        CASPAR_LOG(error) << "Failed to allocate GstBuffer";
        return nullptr;
    }
    
    // Map buffer for writing
    GstMapInfo map;
    if (!gst_buffer_map(buffer, &map, GST_MAP_WRITE)) {
        gst_buffer_unref(buffer);
        CASPAR_LOG(error) << "Failed to map GstBuffer for writing";
        return nullptr;
    }
    
    // Copy data from frame to buffer
    switch (pix_desc.format) {
        case core::pixel_format::bgra:
        case core::pixel_format::rgba:
        case core::pixel_format::argb:
        case core::pixel_format::abgr:
        case core::pixel_format::rgb:
        case core::pixel_format::bgr: {
            // For packed formats with a single plane
            auto plane = pix_desc.planes[0];
            int line_size = info.stride[0];
            
            int plane_height = static_cast<int>(plane.height);
            
            tbb::parallel_for(0, plane_height, [&](int y) {
                std::memcpy(
                    map.data + y * line_size,
                    frame.image_data(0).begin() + y * plane.linesize,
                    plane.linesize);
            });
            break;
        }
        case core::pixel_format::ycbcr: {
            // For planar YUV formats
            for (int p = 0; p < 3; ++p) {
                auto plane = pix_desc.planes[p];
                int offset = info.offset[p];
                int stride = info.stride[p];
                
                int plane_height = static_cast<int>(plane.height);
                
                tbb::parallel_for(0, plane_height, [&](int y) {
                    std::memcpy(
                        map.data + offset + y * stride,
                        frame.image_data(p).begin() + y * plane.linesize,
                        plane.linesize);
                });
            }
            break;
        }
        case core::pixel_format::ycbcra: {
            // For planar YUV formats with alpha
            for (int p = 0; p < 4; ++p) {
                auto plane = pix_desc.planes[p];
                int offset = info.offset[p];
                int stride = info.stride[p];
                
                int plane_height = static_cast<int>(plane.height);
                
                tbb::parallel_for(0, plane_height, [&](int y) {
                    std::memcpy(
                        map.data + offset + y * stride,
                        frame.image_data(p).begin() + y * plane.linesize,
                        plane.linesize);
                });
            }
            break;
        }
        default:
            // Handle other formats
            break;
    }
    
    // Set buffer timing info
    GST_BUFFER_PTS(buffer) = GST_CLOCK_TIME_NONE;
    GST_BUFFER_DTS(buffer) = GST_CLOCK_TIME_NONE;
    GST_BUFFER_DURATION(buffer) = GST_CLOCK_TIME_NONE;
    
    gst_buffer_unmap(buffer, &map);
    
    // Create caps
    GstCaps* caps = gst_video_info_to_caps(&info);
    
    // Create sample
    GstSample* sample = gst_sample_new(buffer, caps, nullptr, nullptr);
    
    // Clean up
    gst_buffer_unref(buffer);
    gst_caps_unref(caps);
    
    return sample;
}

gst_ptr<GstElement> create_pipeline(const std::string& pipeline_description)
{
    GError* error = nullptr;
    GstElement* pipeline = gst_parse_launch(pipeline_description.c_str(), &error);
    
    if (error) {
        std::string error_msg = error->message;
        g_error_free(error);
        CASPAR_THROW_EXCEPTION(gstreamer_error_t() 
                              << gstreamer_error_info("Failed to create pipeline: " + error_msg)
                              << boost::errinfo_api_function("gst_parse_launch"));
    }
    
    return make_gst_ptr<GstElement>(pipeline);
}

std::map<std::string, std::string> parse_gst_structure(GstStructure* structure)
{
    std::map<std::string, std::string> result;
    
    if (!structure)
        return result;
        
    const char* name = gst_structure_get_name(structure);
    result["name"] = name ? name : "";
    
    int num_fields = gst_structure_n_fields(structure);
    for (int i = 0; i < num_fields; ++i) {
        const char* field_name = gst_structure_nth_field_name(structure, i);
        if (!field_name)
            continue;
            
        // Get the value directly - only 2 parameters
        const GValue* val = gst_structure_get_value(structure, field_name);
        if (!val)
            continue;
            
        if (G_VALUE_TYPE(val) == G_TYPE_STRING) {
            result[field_name] = g_value_get_string(val);
        } else {
            gchar* str = gst_value_serialize(val);
            if (str) {
                result[field_name] = str;
                g_free(str);
            }
        }
    }
    
    return result;
}

std::string caps_to_string(GstCaps* caps)
{
    if (!caps)
        return "NULL";
        
    gchar* caps_str = gst_caps_to_string(caps);
    std::string result = caps_str;
    g_free(caps_str);
    
    return result;
}

}} // namespace caspar::gstreamer

#ifdef _MSC_VER
#pragma warning(pop)
#endif
