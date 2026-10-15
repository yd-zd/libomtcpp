/**
 * OMTChannel.cpp - Client Connection Channel Implementation
 * Port of libomtnet/src/OMTChannel.cs
 */

#include "OMTChannel.h"
#include <cstring>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <poll.h>
#include <string>

#ifdef __ANDROID__
#include <android/log.h>
#define LOG_TAG "omt-channel"
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO, LOG_TAG, __VA_ARGS__)
#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, __VA_ARGS__)
#define LOGD(...) __android_log_print(ANDROID_LOG_DEBUG, LOG_TAG, __VA_ARGS__)
#else
#include <cstdio>
#define LOGI(...) do{std::fprintf(stderr,"[omt][I] " __VA_ARGS__);std::fprintf(stderr,"\n");}while(0)
#define LOGE(...) do{std::fprintf(stderr,"[omt][E] " __VA_ARGS__);std::fprintf(stderr,"\n");}while(0)
#define LOGW(...) do{std::fprintf(stderr,"[omt][W] " __VA_ARGS__);std::fprintf(stderr,"\n");}while(0)
#define LOGD(...) do{std::fprintf(stderr,"[omt][D] " __VA_ARGS__);std::fprintf(stderr,"\n");}while(0)
#endif

namespace omt {

static_assert(sizeof(FrameHeader) == 16, "FrameHeader wire layout must stay 16 bytes");
static_assert(sizeof(VideoExtHeader) == 32, "VideoExtHeader wire layout must stay 32 bytes");

namespace {
// Largest control (Metadata) payload the receive path will parse. metadataLength
// is uint16 (<=65535); a full metadata frame is 16 + dataLength bytes. recvBuffer
// is sized for this so a well-formed frame always fits and a larger claim is
// rejected before it can force a zero-capacity recv().
constexpr int32_t kMaxControlPayload = 65535;

// Poll-driven non-blocking full send. Blocking semantics WITHOUT touching the
// shared fd flags (no fcntl(F_SETFL) anywhere): POLLOUT readiness + MSG_DONTWAIT.
bool sendAllPoll(int fd, const uint8_t* data, size_t length,
                 const std::atomic<bool>& alive, int timeoutMs = 5000) {
    size_t off = 0;
    // P0 #7 (Codex): bound the write. A peer that accepts the socket but never
    // reads keeps poll() returning 0 forever (alive stays true), so an unbounded
    // loop would hang the caller — and for the SYNC metadata path, the caller
    // holds channelsMutex_ across this, stalling the whole sender. Fail after a
    // deadline instead so the channel can be torn down.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs);
    while (off < length && alive.load()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            LOGW("sendAllPoll: timed out after %d ms at %zu/%zu bytes (peer not draining)",
                 timeoutMs, off, length);
            return false;
        }
        struct pollfd pfd{fd, POLLOUT, 0};
        const int pr = poll(&pfd, 1, 100);
        if (pr < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (pr == 0) continue;  // timeout — re-check alive + deadline
        const ssize_t n = ::send(fd, data + off, length - off,
                                 MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0) { off += static_cast<size_t>(n); continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
            continue;
        return false;
    }
    return off == length;
}
} // namespace

Channel::Channel(int socketFd, struct sockaddr_in address, 
                 int sendBufferSize, IChannelListener* listener,
                 int sendQueueCount)
    : socketFd_(socketFd), address_(address), listener_(listener) {
    // P0 #19: clamp the configurable async send pool depth to a sane range.
    sendQueueCount_ = (sendQueueCount < 1) ? 1
                    : (sendQueueCount > 16) ? 16 : sendQueueCount;
        LOGI("Channel: created on fd=%d (FD_SETSIZE=%d)", socketFd, FD_SETSIZE);
    
    // Non-blocking ONCE at construction; nothing may flip these flags later
    // (sendAllPoll/MSG_DONTWAIT carry the per-call semantics — see 2026-10-13 fix).
    int flags = fcntl(socketFd_, F_GETFL, 0);
    fcntl(socketFd_, F_SETFL, flags | O_NONBLOCK);
    
    // TCP_NODELAY for low latency
    int one = 1;
    setsockopt(socketFd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    
    // Set send buffer
    setSendBuffer(sendBufferSize);
    
    // TCP keepalive
    setsockopt(socketFd_, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
    
    // TCP_KEEPIDLE = 5 seconds (matches official libomtnet)
#ifdef TCP_KEEPIDLE
    int keepIdle = 5;
    setsockopt(socketFd_, IPPROTO_TCP, TCP_KEEPIDLE, &keepIdle, sizeof(keepIdle));
#endif
    
    // Create async send pool (P0 #19: depth configurable via SenderConfig)
    sendPool_ = std::make_unique<AsyncPool>(
        sendQueueCount_, 
        Constants::NETWORK_ASYNC_BUFFER_AV);
    
    // Start threads
    // Start threads
    senderThread_ = std::thread(&Channel::senderLoop, this);
    receiverThread_ = std::thread(&Channel::receiverLoop, this);
    
    // Initial metadata handshake is now handled by Sender
    
    LOGI("Channel: connected %s:%d", getRemoteAddress().c_str(), getRemotePort());
}

Channel::~Channel() {
    LOGI("Channel: destroying");
    running_ = false;
    connected_ = false;
    
    // Shutdown socket to unblock threads (do not close yet — threads may still
    // reference the descriptor; the single close happens below, after joins).
    const int livefd = socketFd_.load();
    if (livefd >= 0) {
        shutdown(livefd, SHUT_RDWR);
    }
    
    queueCV_.notify_all();
    
    if (receiverThread_.joinable()) {
        receiverThread_.join();
    }
    if (senderThread_.joinable()) {
        senderThread_.join();
    }
    
    // Clear pending queue
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        while (!pendingQueue_.empty()) {
            sendPool_->release(pendingQueue_.front());
            pendingQueue_.pop();
        }
    }
    
    closeSocket(); // P0 #5: the single, final close — after both threads joined
    
    LOGI("Channel: destroyed, sent=%lld, dropped=%lld, stale-evicted=%lld", 
         (long long)framesSent_.load(), (long long)framesDropped_.load(),
         (long long)staleEvicted_.load());
}

void Channel::disconnect() {
    // P0 #5 (Codex): signal stop and unblock the socket, but do NOT close it
    // here — the sender/receiver threads may still be using the descriptor, and
    // another socket could reuse that number. The final owner (~Channel) joins
    // both threads and closes exactly once.
    connected_ = false;
    running_ = false;
    const int fd = socketFd_.load();
    if (fd >= 0)
        ::shutdown(fd, SHUT_RDWR);
    queueCV_.notify_all();
}

void Channel::closeSocket() {
    const int fd = socketFd_.exchange(-1);
    if (fd >= 0) {
        close(fd);
    }
}

void Channel::setSendBuffer(int size) {
    setsockopt(socketFd_, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    
    int actualBuf = 0;
    socklen_t optlen = sizeof(actualBuf);
    getsockopt(socketFd_, SOL_SOCKET, SO_SNDBUF, &actualBuf, &optlen);
    LOGI("Channel: SO_SNDBUF=%d (requested %d)", actualBuf, size);
}

std::string Channel::getRemoteAddress() const {
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &address_.sin_addr, buf, sizeof(buf));
    return std::string(buf);
}

int Channel::getRemotePort() const {
    return ntohs(address_.sin_port);
}

ChannelStats Channel::getStats() const {
    return {
        framesSent_.load(),
        framesDropped_.load(),
        bytesSent_.load(),
        bytesReceived_.load()
    };
}

bool Channel::sendMetadataSync(const char* xml) {
    if (!connected_) return false;
    
    int len = strlen(xml);
    int totalLen = len + 1;
    
    FrameHeader header{};
    header.version = 1;
    header.frameType = static_cast<uint8_t>(FrameType::Metadata);
    header.timestamp = 0;
    header.metadataLength = static_cast<uint16_t>(totalLen);
    header.dataLength = totalLen;
    
    const int fd = socketFd_.load();
    if (fd < 0) return false;

    std::vector<char> frame(sizeof(FrameHeader) + static_cast<size_t>(totalLen));
    memcpy(frame.data(), &header, sizeof(FrameHeader));
    memcpy(frame.data() + sizeof(FrameHeader), xml, len);
    frame[sizeof(FrameHeader) + len] = 0;

    // P0 #7 (Codex): serialize the write across the ENTIRE send (so a concurrent
    // async video frame cannot interleave bytes with this metadata frame on the
    // single TCP stream) AND bound the acquisition: the async writer can hold
    // sendSocketMutex_ for the full sendAllPoll deadline, so we poll try_lock()
    // until a budget then fail + tear the channel down rather than block forever.
    // std::mutex::try_lock (not std::timed_mutex) so ThreadSanitizer models the
    // lock/unlock pairing correctly (timed_mutex's timed path produced spurious
    // "unlock of an unlocked mutex" reports on this libstdc++).
    const auto budget = std::chrono::steady_clock::now() + std::chrono::milliseconds(6000);
    while (!sendSocketMutex_.try_lock()) {
        if (std::chrono::steady_clock::now() >= budget || !connected_.load()) {
            LOGW("sendMetadataSync: could not acquire socket-write lock; disconnecting");
            connected_ = false;
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    const bool ok = sendAllPoll(fd, reinterpret_cast<const uint8_t*>(frame.data()),
                                frame.size(), connected_);
    if (!ok) {
        // P0 #7 (Codex): a PARTIAL write leaves the TCP stream desynchronized
        // (half a metadata frame on the wire); reusing it would mis-frame every
        // subsequent video record. Mark the channel DEAD *while still holding the
        // write lock*, so a waiting async video writer cannot acquire it in the
        // gap, observe a still-connected channel, and append video bytes after the
        // incomplete metadata. Mirrors sendAllBlocking (sets connected_=false under
        // the lock). Only then release the lock.
        LOGW("sendMetadataSync: incomplete metadata write; disconnecting channel");
        connected_ = false;
    }
    sendSocketMutex_.unlock();
    return ok;
}

int Channel::sendAsync(const void* headerData, size_t headerLen,
                       const void* extHeaderData, size_t extHeaderLen,
                       const void* payloadData, size_t payloadLen) {
    if (!connected_) return 0;
    
    size_t totalLen = headerLen + extHeaderLen + payloadLen;
    
    if (totalLen > static_cast<size_t>(Constants::VIDEO_MAX_SIZE)) {
        framesDropped_++;
        LOGE("sendAsync: frame too large %zu", totalLen);
        return 0;
    }
    
    AsyncBuffer* buf = sendPool_->acquire();
    if (!buf) {
        // Drop policy: the OLDEST waiting frame loses, the frame in hand survives.
        //
        // It used to be the opposite — no free buffer meant throwing away THIS frame
        // and keeping whatever was already queued. With the 4-slot / 1 MB ring a
        // subscriber gets (NETWORK_ASYNC_COUNT x NETWORK_ASYNC_BUFFER_AV) and a peer
        // that stalls for a moment, the queue fills with the oldest frames, so when the
        // window reopens senderLoop replays up to 4 already-decoded frames — 133 ms of
        // stale video at 30 fps — before anything live reaches the screen. A momentary
        // stall therefore arrived at the viewer as a permanent latency jump instead of
        // as a couple of lost frames. On a live stream the newest frame is the only one
        // worth queueing: evict the oldest WAITING buffer, hand its slot to this frame,
        // and count the eviction.
        //
        // Safety notes, specific to this hardened desktop copy:
        //  - Only buffers still in pendingQueue_ are eligible. The buffer senderLoop is
        //    currently writing was already popped from that queue under queueMutex_, so
        //    it can never be released here while a send is in flight on it.
        //  - sendSocketMutex_ (P0 #7) is deliberately NOT touched: this path queues, it
        //    never writes to the socket, so metadata/video byte-stream serialization is
        //    unchanged.
        //  - The eviction is a whole-frame removal, so a partial record can never be
        //    left on the wire.
        AsyncBuffer* stale = nullptr;
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            if (!pendingQueue_.empty()) { stale = pendingQueue_.front(); pendingQueue_.pop(); }
        }
        if (stale) {
            sendPool_->release(stale);
            staleEvicted_++;
            framesDropped_++;
            buf = sendPool_->acquire();
        }
    }
    if (!buf) {
        framesDropped_++;
        static int dropLog = 0;
        if (dropLog++ % 30 == 0) {
            LOGD("sendAsync: pool exhausted (dropped: %lld)", 
                 (long long)framesDropped_.load());
        }
        return 0;
    }
    
    buf->resize(totalLen);
    
    size_t offset = 0;
    memcpy(buf->data.data() + offset, headerData, headerLen);
    offset += headerLen;
    memcpy(buf->data.data() + offset, extHeaderData, extHeaderLen);
    offset += extHeaderLen;
    memcpy(buf->data.data() + offset, payloadData, payloadLen);
    buf->length = totalLen;
    
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        pendingQueue_.push(buf);
    }
    queueCV_.notify_one();
    
    return static_cast<int>(totalLen);
}

void Channel::senderLoop() {
    LOGI("senderLoop: started");
    
    while (running_ && connected_) {
        AsyncBuffer* buf = nullptr;
        
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queueCV_.wait_for(lock, std::chrono::milliseconds(100), [this] {
                return !pendingQueue_.empty() || !running_ || !connected_;
            });
            
            if (!running_ || !connected_) break;
            if (pendingQueue_.empty()) continue;
            
            buf = pendingQueue_.front();
            pendingQueue_.pop();
        }
        
        if (!buf) continue;
        
        bool success = sendAllBlocking(buf->data.data(), buf->length);
        
        if (success) {
            framesSent_++;
            bytesSent_ += buf->length;
        }
        
        sendPool_->release(buf);
    }
    
    LOGI("senderLoop: exiting");
}

bool Channel::sendAllBlocking(const uint8_t* data, size_t length) {
    const int fd = socketFd_.load();
    if (fd < 0) return false;
    // P0 #7 (Codex): same per-channel write lock as sendMetadataSync so the
    // async video writer and the sync metadata writer never interleave bytes.
    std::lock_guard<std::mutex> wlock(sendSocketMutex_);
    const bool okSend = sendAllPoll(fd, data, length, connected_);
    if (!okSend)
        connected_ = false;
    return okSend;
}

void Channel::receiverLoop() {
    LOGI("receiverLoop: started");
    
    std::vector<uint8_t> recvBuffer(FrameHeader::SIZE + kMaxControlPayload);
    size_t bufferPos = 0;
    
    while (running_ && connected_) {
        const int fd = socketFd_.load();
        if (fd < 0)
            break; // socket already closed by ~Channel/disconnect
        // poll() has no FD_SETSIZE limit — select() corrupted the stack for
        // fd >= 1024 (proven by ASan socketpair repro, 2026-10-13).
        struct pollfd pfd{fd, POLLIN, 0};
        int sel = ::poll(&pfd, 1, 100);  // 100ms tick
        if (sel < 0) {
            if (errno == EINTR) continue;
            connected_ = false;
            break;
        }
        
        if (sel == 0) continue;
        
        // Never call recv() with zero available capacity: a full buffer means a
        // peer is streaming a frame larger than kMaxControlPayload (already
        // rejected at parse time) or we failed to drain — treat as malformed.
        if (bufferPos >= recvBuffer.size()) {
            LOGE("receiverLoop: receive buffer full (%zu) without a consumable frame", bufferPos);
            connected_ = false;
            break;
        }
        ssize_t received = recv(fd, recvBuffer.data() + bufferPos,
                                recvBuffer.size() - bufferPos, MSG_DONTWAIT);
        
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            // Spurious readability after poll (consumed by a parallel path or
            // SO_RCVLOWAT race) OR an interrupted syscall. NOT a disconnect —
            // keep polling. Treating EAGAIN as peer-close killed every
            // subscribed channel after the subscribe message. Measured 2026-10-13.
            continue;
        }
        if (received < 0) {
            LOGE("receiverLoop: recv error %d", errno);
            connected_ = false;
            break;
        }
        if (received == 0) {
            // P0 #6 (Codex): genuine read-half EOF is a STATE, not a 20ms poll
            // loop. The peer closed its write half (pure receivers shutdown
            // (SHUT_WR) after the subscribe XML). Stop reading here but do NOT
            // mark disconnected — the SEND side (senderLoop) must keep running.
            // A full close surfaces later as a send error (sendAllPoll ->
            // connected_=false) and is reclaimed via cleanupDisconnected.
            // (The old shared `static bool halfCloseLogged` raced across channels.)
            if (!readEof_) {
                LOGI("receiverLoop: peer write-half closed (EOF); stopping reads, keep streaming");
                readEof_ = true;
            }
            break;
        }
        
        bytesReceived_ += received;
        bufferPos += received;
        
        // Process complete frames
        while (bufferPos >= FrameHeader::SIZE) {
            // P0 #3 (Codex): copy the header into a local before dereferencing
            // attacker-controlled fields, and VALIDATE every field BEFORE any
            // arithmetic. dataLength is signed int32: the old
            // `size_t total = FrameHeader::SIZE + header->dataLength` was signed
            // overflow at INT32_MAX, a zero-progress loop at -16 (total==0 never
            // consumed bytes), and a false half-close when an oversized frame
            // filled recvBuffer and the next recv(...,0) returned 0.
            FrameHeader header;
            std::memcpy(&header, recvBuffer.data(), FrameHeader::SIZE);

            const uint8_t ft = header.frameType;
            if (header.version != 1) {
                LOGE("receiverLoop: invalid version %u", header.version);
                connected_ = false;
                break;
            }
            // This is the SENDER's receive path: it only accepts Metadata control
            // frames (subscribe / quality XML) from a peer. Video/Audio arriving
            // here are malformed for this direction — reject rather than parse.
            if (ft != static_cast<uint8_t>(FrameType::Metadata)) {
                LOGE("receiverLoop: unexpected peer frameType %u", ft);
                connected_ = false;
                break;
            }
            if (header.dataLength < 0) {
                LOGE("receiverLoop: negative dataLength %d", header.dataLength);
                connected_ = false;
                break;
            }
            if (header.dataLength > kMaxControlPayload) {
                LOGE("receiverLoop: oversized control frame dataLength %d", header.dataLength);
                connected_ = false;
                break;
            }
            // metadataLength is uint16 and must fit inside dataLength.
            if (static_cast<int32_t>(header.metadataLength) > header.dataLength) {
                LOGE("receiverLoop: metadataLength %u > dataLength %d",
                     header.metadataLength, header.dataLength);
                connected_ = false;
                break;
            }

            const size_t totalFrameLen =
                static_cast<size_t>(FrameHeader::SIZE) + static_cast<size_t>(header.dataLength);

            if (totalFrameLen > bufferPos) break; // wait for the rest of the frame

            // Process metadata
            if (header.dataLength > 0) {
                char* xmlData = reinterpret_cast<char*>(recvBuffer.data() + FrameHeader::SIZE);
                
                // Fix for conflicting implementations:
                // - C# libomtnet sends XML *without* null terminator
                // - Protocol says null terminated
                // - C++ sender sends *with* null terminator
                size_t strLen = header.dataLength;
                if (strLen > 0 && xmlData[strLen - 1] == '\0') {
                    strLen--;
                }
                
                std::string xml(xmlData, strLen);
                processMetadata(xml);
            }
            
            // Shift buffer — every accepted frame consumes >= FrameHeader::SIZE,
            // so this loop always makes progress and cannot spin.
            if (totalFrameLen < bufferPos) {
                memmove(recvBuffer.data(), recvBuffer.data() + totalFrameLen,
                        bufferPos - totalFrameLen);
            }
            bufferPos -= totalFrameLen;
        }
    }
    
    LOGI("receiverLoop: exiting");
    
    // Notify listener
    if (listener_ && !connected_) {
        listener_->onDisconnected(this);
    }
}

void Channel::processMetadata(const std::string& xml) {
    // Subscription commands
    if (xml == MetadataConstants::CHANNEL_SUBSCRIBE_VIDEO) {
        subscriptions_ = subscriptions_.load() | Subscription::Video;
        LOGI("Client subscribed to VIDEO");
        return;
    }
    if (xml == MetadataConstants::CHANNEL_SUBSCRIBE_AUDIO) {
        subscriptions_ = subscriptions_.load() | Subscription::Audio;
        LOGI("Client subscribed to AUDIO");
        return;
    }
    if (xml == MetadataConstants::CHANNEL_SUBSCRIBE_METADATA) {
        subscriptions_ = subscriptions_.load() | Subscription::Metadata;
        LOGI("Client subscribed to METADATA");
        return;
    }
    
    // Tally commands
    if (xml == MetadataConstants::TALLY_PREVIEWPROGRAM) {
        tallyPreview_ = true; tallyProgram_ = true;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    if (xml == MetadataConstants::TALLY_PROGRAM) {
        tallyPreview_ = false; tallyProgram_ = true;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    if (xml == MetadataConstants::TALLY_PREVIEW) {
        tallyPreview_ = true; tallyProgram_ = false;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    if (xml == MetadataConstants::TALLY_NONE) {
        tallyPreview_ = false; tallyProgram_ = false;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    
    // Preview mode
    if (xml == MetadataConstants::CHANNEL_PREVIEW_VIDEO_ON) {
        previewMode_ = true;
        return;
    }
    if (xml == MetadataConstants::CHANNEL_PREVIEW_VIDEO_OFF) {
        previewMode_ = false;
        return;
    }
    
    // Quality suggestion
    if (xml.find(MetadataConstants::SUGGESTED_QUALITY_PREFIX) == 0) {
        Quality oldQuality = suggestedQuality_.load();
        
        if (xml.find("\"Low\"") != std::string::npos) {
            suggestedQuality_ = Quality::Low;
        } else if (xml.find("\"Medium\"") != std::string::npos) {
            suggestedQuality_ = Quality::Medium;
        } else if (xml.find("\"High\"") != std::string::npos) {
            suggestedQuality_ = Quality::High;
        } else {
            suggestedQuality_ = Quality::Default;
        }
        
        if (suggestedQuality_.load() != oldQuality && listener_) {
            listener_->onQualityChanged(this);
        }
        return;
    }
    
    LOGD("Unknown metadata: %s", xml.c_str());
}

} // namespace omt
