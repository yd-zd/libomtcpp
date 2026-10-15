/**
 * OMTChannel.cpp - Client Connection Channel Implementation
 * Port of libomtnet/src/OMTChannel.cs
 */

#include "OMTChannel.h"
#include <cstdarg>
#include <cstring>
#include <cerrno>
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

// The kernel naming the connection dead is the ONLY positive evidence that a peer
// is gone. Everything else — above all a peer that simply stopped reading — is
// back-pressure and must leave the channel alone.
//
// A peer that closed only its WRITE half (a pure receiver that shutdown(SHUT_WR)
// after subscribing, which is what OBS and our own receiver do) is deliberately
// NOT in this list: that state is invisible to poll() and irrelevant to us. It
// becomes PeerGone only if a later send actually errors, which is the difference
// between "the viewer hung up" and "the viewer is a listener with nothing to say".
bool IsPeerGoneErrno(int e) {
    switch (e) {
        case EPIPE:         // peer closed its side; our writes now fail
        case ECONNRESET:    // peer (or a middlebox) sent RST
        case ENOTCONN:      // the connection is already gone
        case ECONNABORTED:  // locally aborted connection
        case ESHUTDOWN:     // send after the connection was shut down
        case EHOSTUNREACH:
        case ENETUNREACH:
        case ENETDOWN:
            return true;
        default:
            return false;    // EAGAIN/ENOMEM/EINVAL/...: transient, never a death sentence
    }
}

// Poll-driven non-blocking full send. Blocking semantics WITHOUT touching the
// shared fd flags (no fcntl(F_SETFL) anywhere): POLLOUT readiness + MSG_DONTWAIT.
//
// `offset` is in/out: pass the frame's length as `length` and the position to
// resume from in `offset`, and a Timeout leaves the number of bytes actually on
// the wire in it, so the caller can hand the SAME buffer back later and finish the
// frame instead of abandoning half of it on the stream. That resume is what makes
// "do not disconnect a slow peer" safe rather than a stream-corruption trade.
//
// Returns WHY it stopped. Timeout is not failure of the peer, it is failure of the
// budget; only PeerGone says anything about the peer.
SendOutcome sendAllPoll(int fd, const uint8_t* data, size_t length,
                        const std::atomic<bool>& alive, size_t& offset,
                        int timeoutMs, int* errOut = nullptr) {
    auto report = [&](SendOutcome o, int err) {
        if (errOut) *errOut = err;
        return o;
    };
    // P0 #7 (Codex): bound the write. A peer that accepts the socket but never
    // reads keeps poll() returning 0 forever (alive stays true), so an unbounded
    // loop would hang the caller — and for the SYNC metadata path, the caller
    // holds channelsMutex_ across this, stalling the whole sender. Fail after the
    // deadline instead. What the caller does with that is the bug this function
    // used to cause: tearing the channel down for a peer that is merely slow.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs);
    while (offset < length && alive.load()) {
        if (std::chrono::steady_clock::now() >= deadline)
            return report(SendOutcome::Timeout, ETIMEDOUT);
        struct pollfd pfd{fd, POLLOUT, 0};
        const int pr = poll(&pfd, 1, 100);
        if (pr < 0) {
            if (errno == EINTR) continue;
            // poll() itself failing is not evidence about the peer. Keep the
            // budget and let a real send() make the call.
            continue;
        }
        if (pr == 0) continue;  // timeout — re-check alive + deadline
        // POLLERR / POLLHUP land here too: we still attempt the send, because the
        // send is what reports the kernel's actual verdict. A half-closed peer that
        // merely shows POLLHUP but still accepts bytes stays alive by design.
        const ssize_t n = ::send(fd, data + offset, length - offset,
                                 MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n > 0) { offset += static_cast<size_t>(n); continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
            continue;
        const int err = (n == 0) ? EPIPE : errno;
        if (n < 0 && IsPeerGoneErrno(err))
            return report(SendOutcome::PeerGone, err);
        // Unknown/transient error: keep burning the bounded budget, do not kill
        // a channel on an errno we do not understand.
    }
    if (offset >= length) return report(SendOutcome::Ok, 0);
    return report(SendOutcome::Timeout, ETIMEDOUT);   // `alive` went false mid-write
}

// Build the on-the-wire bytes of one control (Metadata) frame: the 16-byte
// FrameHeader plus the NUL-terminated XML, exactly as libomtnet expects. Shared by
// the synchronous and the queued path so the two can never disagree on framing.
bool BuildMetadataFrame(const char* xml, std::vector<uint8_t>& out) {
    if (!xml) return false;
    const size_t len = std::strlen(xml);
    if (len + 1 > static_cast<size_t>(kMaxControlPayload)) return false;
    const int totalLen = static_cast<int>(len + 1);

    FrameHeader header{};
    header.version = 1;
    header.frameType = static_cast<uint8_t>(FrameType::Metadata);
    header.timestamp = 0;
    header.metadataLength = static_cast<uint16_t>(totalLen);
    header.dataLength = totalLen;

    out.resize(sizeof(FrameHeader) + static_cast<size_t>(totalLen));
    memcpy(out.data(), &header, sizeof(FrameHeader));
    memcpy(out.data() + sizeof(FrameHeader), xml, len);
    out[sizeof(FrameHeader) + len] = 0;
    return true;
}
} // namespace

Channel::Channel(int socketFd, struct sockaddr_in address, 
                 int sendBufferSize, IChannelListener* listener,
                 int sendQueueCount, const ChannelBudgets& budgets)
    : socketFd_(socketFd), address_(address), listener_(listener), budgets_(budgets) {
    // P0 #19: clamp the configurable async send pool depth to a sane range.
    sendQueueCount_ = (sendQueueCount < 1) ? 1
                    : (sendQueueCount > 16) ? 16 : sendQueueCount;
        LOGI("Channel: created on fd=%d (FD_SETSIZE=%d)", socketFd, FD_SETSIZE);
    // Every bounded wait starts here: the peer has been connected this long, so a
    // channel that sends nothing at all is never mistaken for a stalled one.
    progressUs_.store(SteadyUs(), std::memory_order_relaxed);
    
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
        std::vector<uint8_t>().swap(pendingMetadata_);
        pendingMetadataActive_ = false;
    }
    // The half-written frame dies with the socket, so it needs no drain — but it
    // must not outlive the channel either (it can hold a whole megabyte).
    {
        std::lock_guard<std::mutex> wlock(sendSocketMutex_);
        clearContinuationLocked();
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

int64_t Channel::SteadyUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

// One line per stall EPISODE, not per attempt. The measured failure this replaces is
// 802 identical "incomplete metadata write; disconnecting channel" lines in three
// minutes of one OBS viewer: a warning that fires on every retry is not a diagnostic,
// it is the outage printed out. The flag re-arms on a fully successful write of the
// same kind, so a genuinely NEW stall is still reported.
void Channel::warnOnce(int kind, const char* fmt, ...) {
    if (kind < 0 || kind > 3) kind = 0;
    if (warnedWrite_[kind].exchange(true, std::memory_order_relaxed)) return;
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    LOGW("%s [channel %s:%d — once per stall, not per attempt]",
         buf, getRemoteAddress().c_str(), getRemotePort());
}

void Channel::clearContinuationLocked() {
    // Swap, not clear(): a 1 MB frame has to actually leave the process, or every
    // channel that ever stalled keeps a megabyte for the rest of its life.
    std::vector<uint8_t>().swap(continuation_);
    continuationOffset_ = 0;
    continuationActive_ = false;
    continuationIsVideo_ = false;
    continuationPending_ = false;
}

void Channel::holdMetadataForRetry(std::vector<uint8_t> frame) {
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (pendingMetadataActive_) {
            // Newest wins. This is the bound: there is ONE slot, so a caller that
            // broadcasts metadata into a stalled channel a thousand times costs one
            // frame of memory and a thousand supersessions, never a thousand frames.
            ++metadataSuperseded_;
        }
        pendingMetadata_ = std::move(frame);
        pendingMetadataActive_ = true;
    }
    queueCV_.notify_one();
}

bool Channel::queueMetadata(const char* xml) {
    std::vector<uint8_t> frame;
    if (!BuildMetadataFrame(xml, frame)) {
        LOGE("queueMetadata: rejected (null or over %d bytes)", kMaxControlPayload);
        return false;
    }
    if (!connected_.load()) return false;
    holdMetadataForRetry(std::move(frame));
    return true;
}

int64_t Channel::queuedMetadataBytes() const {
    std::lock_guard<std::mutex> lock(queueMutex_);
    return pendingMetadataActive_ ? static_cast<int64_t>(pendingMetadata_.size()) : 0;
}

bool Channel::hasPendingWriteForTest() const {
    std::lock_guard<std::mutex> lock(sendSocketMutex_);
    return continuationActive_;
}

// Finish the frame that is already half on the wire. Called with sendSocketMutex_
// held; returns Ok only when the stream is whole again.
SendOutcome Channel::flushContinuationLocked(int fd, int timeoutMs) {
    if (!continuationActive_) return SendOutcome::Ok;
    int err = 0;
    const SendOutcome o = sendAllPoll(fd, continuation_.data(), continuation_.size(),
                                      connected_, continuationOffset_, timeoutMs, &err);
    if (o == SendOutcome::Ok) {
        if (continuationIsVideo_) {
            // The frame was counted neither when it stalled nor twice now: this is
            // the only place it becomes a frame the peer actually owns.
            framesSent_++;
            bytesSent_ += static_cast<int64_t>(continuation_.size());
        }
        warnedWrite_[continuationIsVideo_ ? 0 : 1].store(false, std::memory_order_relaxed);
        noteProgress();
        clearContinuationLocked();
        return SendOutcome::Ok;
    }
    if (o == SendOutcome::PeerGone) {
        LOGE("channel %s:%d: peer gone (%s) while completing a half-written %s frame at %zu/%zu bytes",
             getRemoteAddress().c_str(), getRemotePort(), strerror(err),
             continuationIsVideo_ ? "video" : "metadata",
             continuationOffset_, continuation_.size());
        connected_ = false;   // under the write lock — see the P0 #7 note in sendMetadataSync
        clearContinuationLocked();
        return SendOutcome::PeerGone;
    }
    return SendOutcome::Timeout;   // still nursing it; already warned once when it stalled
}

// Put one complete frame on the wire, or take ownership of the part that is on it.
// Called with sendSocketMutex_ held.
SendOutcome Channel::startFrameLocked(int fd, const uint8_t* data, size_t length,
                                      bool isVideo, int timeoutMs) {
    // A half-written frame owns the byte stream. Starting a second one on top of it
    // would splice two records together and the peer's framer never recovers, so the
    // caller must flush first — this is the invariant that replaced "disconnect
    // instead", and the reason it is safe to keep a slow channel alive.
    if (continuationActive_) return SendOutcome::Timeout;

    size_t off = 0;
    int err = 0;
    const SendOutcome o = sendAllPoll(fd, data, length, connected_, off, timeoutMs, &err);
    if (o == SendOutcome::Ok) {
        warnedWrite_[isVideo ? 0 : 1].store(false, std::memory_order_relaxed);
        noteProgress();
        return SendOutcome::Ok;
    }
    if (o == SendOutcome::PeerGone) {
        LOGE("channel %s:%d: peer gone (%s) after %zu/%zu bytes of a %s frame",
             getRemoteAddress().c_str(), getRemotePort(), strerror(err), off, length,
             isVideo ? "video" : "metadata");
        connected_ = false;   // under the write lock, before any waiter can proceed
        clearContinuationLocked();
        return SendOutcome::PeerGone;
    }
    // Timeout: the peer is alive and simply not draining. Keep the bytes already
    // committed as the thing to finish next, and say so ONCE.
    continuation_.assign(data, data + length);
    continuationOffset_ = off;
    continuationActive_ = true;
    continuationIsVideo_ = isVideo;
    continuationPending_ = true;
    if (off > 0) noteProgress();   // bytes we just wrote ARE progress, even if the frame did not finish
    warnOnce(isVideo ? 0 : 1,
             "%s: peer not draining — %zu/%zu bytes of a %s frame on the socket after %d ms; "
             "channel KEPT, the remainder finishes when the peer reads",
             isVideo ? "senderLoop" : "sendMetadataSync", off, length,
             isVideo ? "video" : "metadata", timeoutMs);
    queueCV_.notify_one();
    return SendOutcome::Timeout;
}

bool Channel::sendMetadataSync(const char* xml) {
    if (!connected_) return false;

    std::vector<uint8_t> frame;
    if (!BuildMetadataFrame(xml, frame)) {
        LOGE("sendMetadataSync: rejected (null or over %d bytes)", kMaxControlPayload);
        return false;
    }

    const int fd = socketFd_.load();
    if (fd < 0) return false;

    // P0 #7 (Codex): serialize the write across the ENTIRE send (so a concurrent
    // async video frame cannot interleave bytes with this metadata frame on the
    // single TCP stream) AND bound the acquisition: the async writer can hold
    // sendSocketMutex_ for the full sendAllPoll deadline, so we poll try_lock()
    // until a budget rather than block forever.
    // std::mutex::try_lock (not std::timed_mutex) so ThreadSanitizer models the
    // lock/unlock pairing correctly (timed_mutex's timed path produced spurious
    // "unlock of an unlocked mutex" reports on this libstdc++).
    //
    // Failing that budget is BACK-PRESSURE, not death. The writer holding the lock
    // that long means the peer is not draining the socket — which says nothing about
    // whether the peer exists. The old code set connected_=false here and dropped
    // the frame; both were wrong: the channel stays, and the banner is parked in the
    // single waiting-metadata slot so it goes out the moment the socket opens.
    const auto budget = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(budgets_.lockBudgetMs);
    while (!sendSocketMutex_.try_lock()) {
        if (!connected_.load()) return false;
        if (std::chrono::steady_clock::now() >= budget) {
            warnOnce(2, "sendMetadataSync: could not acquire the socket-write lock within %d ms "
                        "(the async writer is blocked on a peer that is not draining); "
                        "channel KEPT, metadata queued for retry",
                     budgets_.lockBudgetMs);
            holdMetadataForRetry(std::move(frame));
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    SendOutcome outcome = SendOutcome::Ok;
    if (continuationActive_) {
        // Cannot start a new frame while another is half on the wire. Try to finish
        // it; if that is not possible inside the flush budget, this is still just
        // back-pressure, so keep the channel and queue the banner behind it.
        outcome = flushContinuationLocked(fd, budgets_.flushBudgetMs);
        if (outcome != SendOutcome::Ok) {
            if (outcome == SendOutcome::PeerGone) {
                sendSocketMutex_.unlock();     // already marked disconnected under the lock
                return false;
            }
            warnOnce(1, "sendMetadataSync: a half-written frame owns the byte stream; "
                        "channel KEPT, metadata queued for retry");
            holdMetadataForRetry(std::move(frame));
            sendSocketMutex_.unlock();
            return false;
        }
    }

    ++metadataAttempts_;
    outcome = startFrameLocked(fd, frame.data(), frame.size(), /*isVideo=*/false,
                               budgets_.sendTimeoutMs);
    // P0 #7 (Codex): a PARTIAL write leaves the TCP stream desynchronized (half a
    // metadata frame on the wire); reusing it would mis-frame every subsequent video
    // record. That is why the dead path marks the channel disconnected WHILE STILL
    // HOLDING the write lock, so a waiting async video writer cannot acquire it in
    // the gap, observe a still-connected channel, and append video bytes after the
    // incomplete metadata — startFrameLocked/flushContinuationLocked both do it, and
    // both then release below. A TIMEOUT is the opposite case: the bytes we wrote are
    // owned by the continuation, the channel survives, and the caller may retry.
    sendSocketMutex_.unlock();
    return outcome == SendOutcome::Ok;
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
        std::vector<uint8_t> meta;
        bool haveMeta = false;
        bool haveWork = false;
        bool stalledOut = false;

        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queueCV_.wait_for(lock, std::chrono::milliseconds(100), [this] {
                return !pendingQueue_.empty() || !running_ || !connected_
                       || pendingMetadataActive_ || continuationPending_.load();
            });
            
            if (!running_ || !connected_) break;

            const bool queueing = !pendingQueue_.empty();
            const bool waiting  = pendingMetadataActive_;
            const bool partial  = continuationPending_.load();
            haveWork = queueing || waiting || partial;

            // A half-written frame owns the byte stream, so nothing NEW may start
            // while it waits — and the queue is deliberately left ALONE in that
            // case. Popping here would discard the newest queued frame in exchange
            // for bytes we are not allowed to write yet, which inverts the
            // drop-oldest policy the ring is built around.
            if (haveWork && !partial) {
                if (queueing) {
                    buf = pendingQueue_.front();
                    pendingQueue_.pop();
                } else if (waiting) {
                    meta = std::move(pendingMetadata_);
                    pendingMetadata_.clear();
                    pendingMetadataActive_ = false;
                    haveMeta = true;
                }
            }

            // Liveness, as distinct from back-pressure: work to send and not ONE
            // byte accepted for budgets_.stalledPeerMs is no longer a slow viewer,
            // it is a viewer that is gone and has not said so. Without this gate the
            // fix below would trade "kills healthy channels 4.5 times a second" for
            // "never reaps a frozen one"; TCP keepalive alone is 5s idle + 75s x 9
            // probes before it can error a socket, which is not a bound anyone
            // operating a broadcast box would accept. qa-ompin/wire-slow-reader.cpp
            // case 2 is the scenario where this line is the only thing standing.
            if (haveWork) {
                const int64_t idleUs = SteadyUs() - progressUs_.load(std::memory_order_relaxed);
                stalledOut = idleUs > static_cast<int64_t>(budgets_.stalledPeerMs) * 1000;
            }
            if (!haveWork) continue;   // idle tick, nothing pending anywhere
        }

        if (stalledOut) {
            if (buf) sendPool_->release(buf);
            warnOnce(3, "no byte of ours reached the socket for %d ms while %s was queued; "
                       "treating the peer as gone",
                     budgets_.stalledPeerMs, haveMeta ? "metadata" : "video");
            connected_ = false;
            break;
        }

        std::unique_lock<std::mutex> wlock(sendSocketMutex_);
        const int fd = socketFd_.load();
        bool keepWriting = true;
        bool requeueMeta = false;
        bool bufWritten = false;
        bool bufLost = false;
        {
            if (fd < 0) {
                requeueMeta = haveMeta;
                keepWriting = false;
                bufLost = (buf != nullptr);
            } else {
                SendOutcome outcome = SendOutcome::Ok;
                if (continuationActive_)
                    outcome = flushContinuationLocked(fd, budgets_.flushBudgetMs);

                if (outcome == SendOutcome::Ok && buf) {
                    const std::int64_t len = static_cast<std::int64_t>(buf->length);
                    outcome = startFrameLocked(fd, buf->data.data(), buf->length, /*isVideo=*/true,
                                               budgets_.sendTimeoutMs);
                    // A TIMEOUT means the frame is RETAINED as the continuation: it is counted
                    // when it completes, never twice. Only a frame that is genuinely gone — the
                    // connection died under it — is a drop here.
                    if (outcome == SendOutcome::Ok) { framesSent_++; bytesSent_ += len; bufWritten = true; }
                    else if (outcome == SendOutcome::PeerGone) { framesDropped_++; bufWritten = true; }
                } else if (outcome == SendOutcome::Ok && haveMeta) {
                    ++metadataAttempts_;
                    outcome = startFrameLocked(fd, meta.data(), meta.size(), /*isVideo=*/false,
                                               budgets_.sendTimeoutMs);
                    // A metadata frame the socket would not take is not lost, it is waiting:
                    // back it onto the single slot rather than drop the banner the viewer has
                    // still not seen. PeerGone is the exception — nothing is waiting for a dead
                    // channel, and connected_ was already cleared under the write lock.
                    requeueMeta = (outcome != SendOutcome::Ok) && connected_.load();
                }
                (void)outcome;   // PeerGone already cleared connected_ inside the locked helpers
            }
            if (buf && !bufWritten) bufLost = true;   // never even started: the stream was busy
        }
        wlock.unlock();
        // Every acquired buffer is released exactly once, on every path — a buffer stranded
        // here would shrink the ring permanently and eventually stop the channel sending.
        if (buf) {
            if (bufLost) framesDropped_++;
            sendPool_->release(buf);
        }
        // Re-queue OUTSIDE the write lock: holdMetadataForRetry takes queueMutex_, and
        // sendSocketMutex_ -> queueMutex_ must never be a nesting this class relies on.
        if (requeueMeta) holdMetadataForRetry(std::move(meta));
        if (!keepWriting) break;
    }
    
    LOGI("senderLoop: exiting");
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
