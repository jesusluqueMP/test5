/*
 * This file is part of CasparCG (www.casparcg.com).
 *
 * CasparCG is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CasparCG is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CasparCG. If not, see <http://www.gnu.org/licenses/>.
 */

 #include "../StdAfx.h"

 #include "gstreamer_producer.h"
 #include "gst_producer.h"
 
 #include <common/env.h>
 #include <common/os/filesystem.h>
 #include <common/param.h>
 
 #include <core/frame/draw_frame.h>
 #include <core/frame/frame_factory.h>
 #include <core/frame/geometry.h>
 #include <core/producer/frame_producer.h>
 #include <core/video_format.h>
 
 #include <boost/algorithm/string/predicate.hpp>
 #include <boost/filesystem.hpp>
 #include <boost/filesystem/fstream.hpp>
 #include <boost/logic/tribool.hpp>
 #include <common/filesystem.h>
 
 namespace caspar { namespace gstreamer {
 
 struct gstreamer_producer : public core::frame_producer
 {
     const std::wstring                   filename_;
     spl::shared_ptr<core::frame_factory> frame_factory_;
     core::video_format_desc              format_desc_;
 
     std::shared_ptr<GstProducer> producer_;
 
   public:
     explicit gstreamer_producer(spl::shared_ptr<core::frame_factory> frame_factory,
                              core::video_format_desc              format_desc,
                              std::wstring                         path,
                              std::wstring                         filename,
                              std::wstring                         vfilter,
                              std::optional<int64_t>               start,
                              std::optional<int64_t>               seek,
                              std::optional<int64_t>               duration,
                              std::optional<bool>                  loop,
                              core::frame_geometry::scale_mode     scale_mode)
         : filename_(filename)
         , frame_factory_(frame_factory)
         , format_desc_(format_desc)
         , producer_(new GstProducer(frame_factory_,
                                    format_desc_,
                                    u8(filename),
                                    u8(path),
                                    u8(vfilter),
                                    start,
                                    seek,
                                    duration,
                                    loop,
                                    scale_mode))
     {
     }
 
     ~gstreamer_producer()
     {
         std::thread([producer = std::move(producer_)]() mutable {
             try {
                 producer.reset();
             } catch (...) {
                 CASPAR_LOG_CURRENT_EXCEPTION();
             }
         }).detach();
     }
 
     // frame_producer
 
     core::draw_frame last_frame(const core::video_field field) override { return producer_->prev_frame(field); }
 
     core::draw_frame receive_impl(const core::video_field field, int nb_samples) override
     {
         return producer_->next_frame(field);
     }
 
     std::uint32_t frame_number() const override
     {
         return static_cast<std::uint32_t>(producer_->time() - producer_->start());
     }
 
     std::uint32_t nb_frames() const override
     {
         return producer_->loop() ? std::numeric_limits<std::uint32_t>::max()
                                  : static_cast<std::uint32_t>(producer_->duration());
     }
 
     bool is_ready() override { return producer_->is_ready(); }
 
     std::future<std::wstring> call(const std::vector<std::wstring>& params) override
     {
         std::wstring result;
 
         std::wstring cmd = params.at(0);
         std::wstring value;
         if (params.size() > 1) {
             value = params.at(1);
         }
 
         if (boost::iequals(cmd, L"loop")) {
             if (!value.empty()) {
                 producer_->loop(boost::lexical_cast<bool>(value));
             }
 
             result = std::to_wstring(producer_->loop());
         } else if (boost::iequals(cmd, L"in") || boost::iequals(cmd, L"start")) {
             if (!value.empty()) {
                 producer_->start(boost::lexical_cast<int64_t>(value));
             }
 
             result = std::to_wstring(producer_->start());
         } else if (boost::iequals(cmd, L"out")) {
             if (!value.empty()) {
                 producer_->duration(boost::lexical_cast<int64_t>(value) - producer_->start());
             }
 
             result = std::to_wstring(producer_->start() + producer_->duration());
         } else if (boost::iequals(cmd, L"length")) {
             if (!value.empty()) {
                 producer_->duration(boost::lexical_cast<std::int64_t>(value));
             }
 
             result = std::to_wstring(producer_->duration());
         } else if (boost::iequals(cmd, L"seek") && !value.empty()) {
             int64_t seek;
             if (boost::iequals(value, L"rel")) {
                 seek = producer_->time();
             } else if (boost::iequals(value, L"in")) {
                 seek = producer_->start();
             } else if (boost::iequals(value, L"out")) {
                 seek = producer_->start() + producer_->duration();
             } else if (boost::iequals(value, L"end")) {
                 seek = producer_->duration();
             } else {
                 seek = boost::lexical_cast<int64_t>(value);
             }
 
             if (params.size() > 2) {
                 seek += boost::lexical_cast<int64_t>(params.at(2));
             }
 
             producer_->seek(seek);
 
             result = std::to_wstring(seek);
         } else {
             CASPAR_THROW_EXCEPTION(invalid_argument());
         }
 
         std::promise<std::wstring> promise;
         promise.set_value(result);
         return promise.get_future();
     }
 
     std::wstring print() const override
     {
         const int64_t position = std::max(producer_->time() - producer_->start(), INT64_C(0));
         return L"gstreamer[" + filename_ + L"|" + std::to_wstring(position) + L"/" +
                std::to_wstring(producer_->duration()) + L"]";
     }
 
     std::wstring name() const override { return L"gstreamer"; }
 
     core::monitor::state state() const override { return producer_->state(); }
 };
 
 static bool is_valid_gstreamer_file(const boost::filesystem::path& path)
 {
     static const std::set<std::wstring> valid_extensions = {
         L".mov", L".mp4", L".dv", L".flv", L".mpg", L".mkv", L".mxf", L".ts", L".mp3", L".wav", 
         L".wma", L".nut", L".flac", L".opus", L".ogg", L".webm"
     };
     static const std::set<std::wstring> valid_protocols = {
         L"rtmp://", L"rtmps://", L"http://", L"https://", L"mms://", L"rtp://", L"udp://"
     };
     
     auto ext = boost::to_lower_copy(path.extension().wstring());
     
     // Check if it's a recognized file extension
     if (valid_extensions.find(ext) != valid_extensions.end()) {
         return true;
     }
     
     // Check if it's a recognized streaming protocol
     auto pathstr = path.wstring();
     for (const auto& protocol : valid_protocols) {
         if (boost::algorithm::istarts_with(pathstr, protocol)) {
             return true;
         }
     }
     
     return false;
 }
 
 spl::shared_ptr<core::frame_producer> create_producer(const core::frame_producer_dependencies& dependencies,
                                                       const std::vector<std::wstring>&         params)
 {
     auto name = params.at(0);
     auto path = name;
 
     if (!boost::contains(path, L"://")) {
         auto fullMediaPath = find_file_within_dir_or_absolute(env::media_folder(), path, is_valid_gstreamer_file);
         if (fullMediaPath) {
             path = fullMediaPath->wstring();
         } else {
             return core::frame_producer::empty();
         }
     } else if (!is_valid_gstreamer_file(path)) {
         return core::frame_producer::empty();
     }
 
     if (path.empty()) {
         return core::frame_producer::empty();
     }
 
     auto loop = contains_param(L"LOOP", params);
 
     auto seek = get_param(L"SEEK", params, static_cast<uint32_t>(0));
     auto in   = get_param(L"IN", params, seek);
 
     if (!contains_param(L"SEEK", params)) {
         // Default to the same when only one is defined
         seek = in;
     }
 
     auto out = get_param(L"LENGTH", params, std::numeric_limits<uint32_t>::max());
     if (out < std::numeric_limits<uint32_t>::max() - in)
         out += in;
     else
         out = std::numeric_limits<uint32_t>::max();
     out = get_param(L"OUT", params, out);
 
     auto filter_str = get_param(L"FILTER", params, L"");
     
     auto scale_mode = core::scale_mode_from_string(get_param(L"SCALE_MODE", params, L"STRETCH"));
 
     std::optional<std::int64_t> start;
     std::optional<std::int64_t> seek2;
     std::optional<std::int64_t> duration;
 
     if (in != 0) {
         start = in;
     }
     if (seek != 0) {
         seek2 = seek;
     }
 
     if (out != std::numeric_limits<uint32_t>::max()) {
         duration = out - in;
     }
 
     auto vfilter = get_param(L"VF", params, filter_str);
 
     try {
         return spl::make_shared<gstreamer_producer>(dependencies.frame_factory,
                                                   dependencies.format_desc,
                                                   name,
                                                   path,
                                                   vfilter,
                                                   start,
                                                   seek2,
                                                   duration,
                                                   loop,
                                                   scale_mode);
     } catch (...) {
         CASPAR_LOG_CURRENT_EXCEPTION();
     }
     return core::frame_producer::empty();
 }
 
 }} // namespace caspar::gstreamer