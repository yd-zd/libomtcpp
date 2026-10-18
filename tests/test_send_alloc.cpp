#include <iostream>
#include <vector>
#include <cstdlib>
#include <atomic>
#include <chrono>
#include <cassert>
#include <thread>
#include "OMTSender.h"

static thread_local int64_t t_mallocCount = 0;
static thread_local int64_t t_freeCount = 0;
static thread_local bool t_trackAllocations = false;

void* operator new(std::size_t size) {
    if (t_trackAllocations) t_mallocCount++;
    void* ptr = std::malloc(size);
    if (!ptr) throw std::bad_alloc();
    return ptr;
}

void operator delete(void* ptr) noexcept {
    if (t_trackAllocations) t_freeCount++;
    std::free(ptr);
}

void operator delete(void* ptr, std::size_t) noexcept {
    if (t_trackAllocations) t_freeCount++;
    std::free(ptr);
}

int main() {
    std::cout << "Starting Thread-Isolated Allocation Test..." << std::endl;

    omt::SenderConfig config;
    config.name = "Test Sender";
    config.port = 16401;

    omt::Sender sender(config);
    if (!sender.start()) {
        std::cerr << "Failed to start sender!" << std::endl;
        return 1;
    }

    int width = 1280;
    int height = 720;
    int stride = width * 2; // UYVY
    std::vector<uint8_t> frameData(stride * height, 0x80);

    omt::MediaFrame frame{};
    frame.data = frameData.data();
    frame.dataLength = static_cast<int>(frameData.size());
    frame.width = width;
    frame.height = height;
    frame.stride = stride;
    frame.codec = omt::Codec::UYVY;
    frame.frameRateN = 60;
    frame.frameRateD = 1;
    frame.aspectRatio = 16.0f / 9.0f;
    frame.colorSpace = omt::ColorSpace::BT709;
    frame.timestamp = 1000000;

    // Warmup
    for (int i = 0; i < 5; ++i) {
        sender.send(frame);
    }

    // Benchmark thread-local allocations for Sender::send caller thread
    t_mallocCount = 0;
    t_freeCount = 0;
    t_trackAllocations = true;

    const int iterations = 100;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < iterations; ++i) {
        frame.timestamp += 166666;
        sender.send(frame);
    }

    auto end = std::chrono::high_resolution_clock::now();
    t_trackAllocations = false;

    double elapsedUs = std::chrono::duration<double, std::micro>(end - start).count();
    int64_t totalMallocs = t_mallocCount;

    std::cout << "Total Thread-Local Mallocs in 100 Sender::send calls: " << totalMallocs << std::endl;
    std::cout << "Avg Mallocs per Sender::send call: " << static_cast<double>(totalMallocs) / iterations << std::endl;
    std::cout << "Avg Time/frame: " << elapsedUs / iterations << " us" << std::endl;

    sender.stop();

    if (totalMallocs == 0) {
        std::cout << "SUCCESS: Zero thread-local heap allocations in Sender::send steady state!" << std::endl;
        return 0;
    } else {
        std::cerr << "FAILURE: Detected " << totalMallocs << " thread-local heap allocations!" << std::endl;
        return 1;
    }
}
