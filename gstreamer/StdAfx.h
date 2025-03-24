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

 #include <algorithm>
 #include <atomic>
 #include <boost/algorithm/string/predicate.hpp>
 #include <boost/exception/all.hpp>
 #include <boost/exception/exception.hpp>
 #include <boost/filesystem.hpp>
 #include <boost/format.hpp>
 #include <boost/lexical_cast.hpp>
 #include <boost/range/algorithm/rotate.hpp>
 #include <boost/rational.hpp>
 #include <cinttypes>
 #include <common/compiler/vs/disable_silly_warnings.h>
 #include <common/diagnostics/graph.h>
 #include <common/env.h>
 #include <common/except.h>
 #include <common/log.h>
 #include <common/memory.h>
 #include <common/os/filesystem.h>
 #include <common/param.h>
 #include <common/scope_exit.h>
 #include <condition_variable>
 #include <core/consumer/frame_consumer.h>
 #include <core/frame/draw_frame.h>
 #include <core/frame/frame.h>
 #include <core/frame/frame_factory.h>
 #include <core/frame/pixel_format.h>
 #include <core/fwd.h>
 #include <core/producer/frame_producer.h>
 #include <core/video_format.h>
 #include <deque>
 #include <exception>
 #include <functional>
 #include <memory>
 #include <mutex>
 #include <optional>
 #include <queue>
 #include <set>
 #include <string>
 #include <tbb/concurrent_queue.h>
 #include <thread>
 #include <vector>
 
 #pragma warning(push, 1)
 
 extern "C" {
 #include <gst/gst.h>
 #include <gst/app/gstappsink.h>
 #include <gst/app/gstappsrc.h>
 #include <gst/video/video.h>
 #include <gst/audio/audio.h>
 }
 
 #pragma warning(pop)