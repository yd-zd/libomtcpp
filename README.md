# libomtcpp

A C++ implementation of the Open Media Transport (OMT) protocol for real-time video streaming.

This library provides the core networking and encoding functionality for OMT senders, allowing applications to stream video to OMT-compatible receivers such as vMix, OBS, and other broadcast software.

## Features

- TCP-based video transport with VMX codec compression
- Multi-client support with per-channel threading
- Quality negotiation (LQ/SQ/HQ profiles)
- Tally support (preview/program indicators)
- Connection metadata and sender information
- Buffer pooling for efficient memory management

## Requirements

- C++17 compatible compiler
- CMake 3.14+
- [libvmx](https://github.com/openmediatransport/libvmx) - VMX video codec

## Building

```bash
# Clone libvmx separately
git clone https://github.com/openmediatransport/libvmx.git ../libvmx

# Build libomtcpp
mkdir build && cd build
cmake .. -DVMX_ROOT=../../libvmx
cmake --build .
```

### Android (NDK)

```bash
cmake .. \
  -DCMAKE_TOOLCHAIN_FILE=$ANDROID_NDK/build/cmake/android.toolchain.cmake \
  -DANDROID_ABI=arm64-v8a \
  -DVMX_ROOT=/path/to/libvmx
```

## Usage

```c
#include "libomt.h"

// Create sender
omt_send_t* sender = omt_send_create("MyCamera", OMTQuality_Medium);

// Send frames (YUV420, NV12, UYVY, or BGRA)
OMTMediaFrame frame = {};
frame.Type = OMTFrameType_Video;
frame.Codec = OMTCodec_NV12;
frame.Width = 1920;
frame.Height = 1080;
frame.Data = yuv_data;
frame.DataLength = data_size;
frame.FrameRateN = 30;
frame.FrameRateD = 1;

omt_send(sender, &frame);

// Cleanup
omt_send_destroy(sender);
```

## mDNS Discovery

libomtcpp provides callback hooks for discovery but does not implement mDNS internally. Use platform-specific solutions:

- **Android**: NsdManager via JNI
- **Linux**: Avahi
- **macOS/Windows**: Bonjour

Discovery callbacks available:
- `omt_discovery_on_service_found()`
- `omt_discovery_on_service_lost()`
- `omt_discovery_on_registered()`

## License

MIT License - See LICENSE.txt
