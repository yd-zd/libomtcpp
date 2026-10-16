# libomtcpp

A native C++17 implementation of the [Open Media Transport (OMT)](https://github.com/openmediatransport) protocol for low-latency video streaming.

Provides TCP transport, VMX video encoding, and multi-client sender functionality for OMT-compatible applications, including vMix, OBS, and broadcast production systems.

## Features

- TCP-based video transport with VMX compression
- Multi-client streaming with per-channel threading
- Quality negotiation (LQ/SQ/HQ)
- Tally support (preview/program)
- Sender identification and connection metadata
- Reusable buffer pools
- C API for integration with native applications
- Android NDK support

## Requirements

- C++17 compiler
- CMake 3.14+
- [libvmx](https://github.com/openmediatransport/libvmx) - VMX video codec

## Building

```bash
# Clone libvmx alongside libomtcpp
git clone https://github.com/openmediatransport/libvmx.git ../libvmx

# Configure and build
cmake -S . -B build -DVMX_ROOT=../libvmx
cmake --build build --config Release
```

### Android (NDK)

```bash
cmake -S . -B build-android \
  -DCMAKE_TOOLCHAIN_FILE="$ANDROID_NDK/build/cmake/android.toolchain.cmake" \
  -DANDROID_ABI=arm64-v8a \
  -DVMX_ROOT=/path/to/libvmx

cmake --build build-android
```

## Usage

```cpp
#include "libomt.h"

// Create sender
omt_send_t* sender = omt_send_create("MyCamera", OMTQuality_Medium);

// Prepare video frame
OMTMediaFrame frame = {};
frame.Type = OMTFrameType_Video;
frame.Codec = OMTCodec_NV12;
frame.Width = 1920;
frame.Height = 1080;
frame.Data = yuv_data;
frame.DataLength = data_size;
frame.FrameRateN = 30;
frame.FrameRateD = 1;

// Send frame
omt_send(sender, &frame);

// Release sender
omt_send_destroy(sender);
```

Supported input formats include YUV420, NV12, UYVY, and BGRA.

## mDNS Discovery

Service discovery is delegated to the host application. The library exposes discovery callbacks but does not include an internal mDNS implementation.

| Platform | Discovery backend |
|---|---|
| Android | NsdManager (JNI) |
| Linux | Avahi |
| macOS / Windows | Bonjour |

Discovery callbacks:

- `omt_discovery_on_service_found()`
- `omt_discovery_on_service_lost()`
- `omt_discovery_on_registered()`

## Engineering Principles

1. **Allocation-Efficient Hot Paths** - Minimize steady-state heap allocations in send, receive, and media-processing loops. Prefer reusable buffers and predictable memory usage.

2. **RAII & Memory Safety** - Enforce deterministic ownership of sockets, threads, and media buffers. Keep C API boundaries exception-safe.

3. **Concurrency & Low Latency** - Minimize contention between network I/O and media processing. Prefer bounded queues and measured synchronization strategies over unnecessary lock-free complexity.

4. **Protocol & Platform Compatibility** - Preserve OMT wire compatibility, public API/ABI contracts, and supported platform behavior.

5. **Measured Optimization** - Prioritize reproducible improvements in throughput, frame latency, memory efficiency, and CPU utilization without compromising correctness.

## License

MIT License. See [LICENSE.txt](LICENSE.txt).
