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

namespace {
// Poll-driven non-blocking full send. Blocking semantics WITHOUT touching the
// shared fd flags (no fcntl(F_SETFL) anywhere): POLLOUT readiness + MSG_DONTWAIT.
bool sendAllPoll(int fd, const uint8_t* data, size_t length,
                 const std::atomic<bool>& alive) {
    size_t off = 0;
    while (off < length && alive.load()) {
        struct pollfd pfd{fd, POLLOUT, 0};
        const int pr = poll(&pfd, 1, 100);
        if (pr < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (pr == 0) continue;  // timeout — re-check alive
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
                 int sendBufferSize, IChannelListener* listener)
    : socketFd_(socketFd), address_(address), listener_(listener) {
    
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
    
    // Create async send pool
    sendPool_ = std::make_unique<AsyncPool>(
        Constants::NETWORK_ASYNC_COUNT, 
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
    
    // Shutdown socket to unblock threads
    if (socketFd_ > 0) {
        shutdown(socketFd_, SHUT_RDWR);
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
    
    closeSocket();
    
    LOGI("Channel: destroyed, sent=%lld, dropped=%lld", 
         (long long)framesSent_.load(), (long long)framesDropped_.load());
}

void Channel::disconnect() {
    connected_ = false;
    running_ = false;
    closeSocket();
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

    return sendAllPoll(fd, reinterpret_cast<const uint8_t*>(frame.data()),
                       frame.size(), connected_);
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
    const bool okSend = sendAllPoll(fd, data, length, connected_);
    if (!okSend)
        connected_ = false;
    return okSend;
}

void Channel::receiverLoop() {
    LOGI("receiverLoop: started");
    
    std::vector<uint8_t> recvBuffer(65536);
    size_t bufferPos = 0;
    
    while (running_ && connected_) {
        const int fd = socketFd_.load();
        if (fd < 0)
            break; // socket already closed by ~Channel/disconnect — see FD_SET guard
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);
        
        struct timeval tv{0, 100000};  // 100ms
        
        int sel = select(fd + 1, &readfds, nullptr, nullptr, &tv);
        
        if (sel < 0) {
            if (errno == EINTR) continue;
            connected_ = false;
            break;
        }
        
        if (sel == 0) continue;
        
        ssize_t received = recv(fd, recvBuffer.data() + bufferPos,
                                recvBuffer.size() - bufferPos, MSG_DONTWAIT);
        
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Spurious readability after select (consumed by a parallel path
            // or SO_RCVLOWAT race). NOT a disconnect — keep polling. Treating
            // EAGAIN as peer-close killed every subscribed channel after the
            // subscribe message (0 frames + later abort). Measured 2026-10-13.
            continue;
        }
        if (received < 0) {
            LOGE("receiverLoop: recv error %d", errno);
            connected_ = false;
            break;
        }
        if (received == 0) {
            // Peer closed its WRITE half (pure receivers shutdown(SHUT_WR) after
            // the subscribe XML). The read half is still valid — WE must keep
            // sending. Treat as idle, NOT as disconnect. Full close surfaces as
            // a later send error (sendAllPoll -> connected_=false), and socket
            // death as recv errors. Verified against omt-recv 2026-10-13.
            static bool halfCloseLogged = false;
            if (!halfCloseLogged) {
                LOGI("receiverLoop: peer write-half closed; keep streaming");
                halfCloseLogged = true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        }
        
        bytesReceived_ += received;
        bufferPos += received;
        
        // Process complete frames
        while (bufferPos >= FrameHeader::SIZE) {
            auto* header = reinterpret_cast<FrameHeader*>(recvBuffer.data());
            
            if (header->version != 1) {
                LOGE("receiverLoop: invalid version %d", header->version);
                connected_ = false;
                break;
            }
            
            size_t totalFrameLen = FrameHeader::SIZE + header->dataLength;
            
            if (totalFrameLen > bufferPos) break;
            
            // Process metadata
            if (header->frameType == static_cast<uint8_t>(FrameType::Metadata) && 
                header->dataLength > 0) {
                char* xmlData = reinterpret_cast<char*>(recvBuffer.data() + FrameHeader::SIZE);
                
                // Fix for conflicting implementations:
                // - C# libomtnet sends XML *without* null terminator
                // - Protocol says null terminated
                // - C++ sender sends *with* null terminator
                size_t strLen = header->dataLength;
                if (strLen > 0 && xmlData[strLen - 1] == '\0') {
                    strLen--;
                }
                
                std::string xml(xmlData, strLen);
                processMetadata(xml);
            }
            
            // Shift buffer
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
