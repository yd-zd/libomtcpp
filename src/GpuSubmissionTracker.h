// GpuSubmissionTracker.h — engine-agnostic tracker for outstanding GPU readback
// submissions (P0 #17, Codex round-3). The node's Impl holds one of these over its
// PendingGpu record + a completion predicate that polls the retained GPU event; the
// native acceptance harness (qa-omp/review-native.cpp) drives the SAME class with a
// trivial record + a fake predicate to prove the bound and the retirement discipline
// WITHOUT needing a real (or mocked) Vulkan device.
//
// Contract the tests assert:
//   * canSubmit() is false once `cap` records are outstanding -> the node must stop
//     allocating/submitting new readback buffers (status GPU_STALLED) rather than
//     grow the parked set without bound on repeated failures/timeouts.
//   * a record is retired (freed) ONLY once its predicate reports completion — never
//     while the GPU may still be writing it (no premature Map/reuse/release).
//   * at teardown, still-unfinished records are handed off (drainUnfinished) so the
//     caller can keep them alive past node deletion (device teardown reclaims them).
#pragma once
#include <cstddef>
#include <functional>
#include <utility>
#include <vector>

namespace omt {

// The outcome the node observed for a just-submitted readback dispatch. Injecting
// this into parkIfUnresolved() lets the native harness exercise the node's PARK
// DECISION (a failed/timed-out submission may still be writing its buffer, so it
// must be kept alive, never reused/freed) without a real or mocked Vulkan device.
enum class SubmissionOutcome { Completed, Failed, TimedOut };

template <class Record>
class GpuSubmissionTracker {
public:
    using CompleteFn = std::function<bool(const Record&)>;

    explicit GpuSubmissionTracker(std::size_t cap) : cap_(cap) {}

    // True while there is room to submit another readback. The caller checks this
    // BEFORE allocating/dispatching; at capacity it must stall (not submit).
    bool canSubmit() const { return records_.size() < cap_; }

    // Park a submission whose GPU work is unresolved (failed/uncertain submit or a
    // wait timeout). It stays alive and is never reused until retired.
    void park(Record r) { records_.push_back(std::move(r)); }

    // The node's post-submission decision, factored here so it is testable by
    // injecting an outcome. A submission that FAILED (RunComputePass/End error or a
    // null event) or TIMED OUT may STILL be writing its buffer, so its record must be
    // PARKED ALIVE (never reused/freed) and the caller forced to allocate a fresh
    // buffer. A COMPLETED submission is NOT parked (its buffer is safe to Map/reuse).
    // Returns true if the record was taken into the tracker (caller must drop its own
    // buffer reference), false if the buffer is usable and nothing was parked.
    bool parkIfUnresolved(Record rec, SubmissionOutcome outcome) {
        if (outcome == SubmissionOutcome::Completed) return false;
        park(std::move(rec));
        return true;
    }
    // Retire every record whose predicate reports the GPU is done, and RETURN them
    // (moved out) so the caller performs the engine-specific free (DestroyBuffer /
    // ReleaseEventHolder). Records still in flight are left untouched.
    std::vector<Record> retireWhere(CompleteFn done) {
        std::vector<Record> retired;
        for (auto it = records_.begin(); it != records_.end();) {
            if (done(*it)) { retired.push_back(std::move(*it)); it = records_.erase(it); }
            else ++it;
        }
        return retired;
    }

    std::size_t size() const { return records_.size(); }
    std::size_t capacity() const { return cap_; }

    // At node teardown: move out every record that has NOT completed, so the caller
    // can transfer ownership (leak) beyond node deletion; completed ones are dropped.
    std::vector<Record> drainUnfinished(CompleteFn done) {
        std::vector<Record> out;
        for (auto& r : records_)
            if (!done(r)) out.push_back(std::move(r));
        records_.clear();
        return out;
    }

private:
    std::size_t cap_;
    std::vector<Record> records_;
};

// The dispatch the node must act on after a readback submission.
enum class ReadbackDispatch {
    ProceedToMap,    // completed: the reusable buffer is intact, caller may Map/reuse
    ParkedFailed,    // submission failed: buffer retained in a parked record, reusable cleared
    ParkedTimeout,   // wait timed out: buffer retained in a parked record, reusable cleared
    Stalled,         // at capacity: caller must not submit further this tick
};

// The node's FULL post-submission handler, factored out of OmpOutNode so the native
// harness drives the PRODUCTION decision with injected engine result codes (this is the
// code the node actually calls; breaking the outcome mapping, the buffer replacement, or
// the park-vs-Map decision now fails the harness). Given the raw RunComputePass / End /
// event / WaitGpuEvent results it: (1) maps them to an outcome; (2) for an UNRESOLVED
// (Failed/TimedOut) submission moves `reusable` into the record (RETENTION — never
// reused/freed), clears the caller's `outReusable` (BUFFER REPLACEMENT — force a fresh
// buffer next tick) and reports Map/reuse is SUPPRESSED (a non-ProceedToMap dispatch);
// (3) for a COMPLETED submission leaves the buffer reusable and returns ProceedToMap.
// `Record` must expose a `.buffer` member assignable from `Buffer`.
template <class Tracker, class Record, class Buffer>
ReadbackDispatch handleReadbackSubmission(
        Tracker& t, Record rec, Buffer reusable,
        int runResult, int endResult, unsigned long long eventValue, int waitResult,
        int successCode, Buffer& outReusable) {
    SubmissionOutcome outcome;
    if (runResult != successCode || endResult != successCode || eventValue == 0)
        outcome = SubmissionOutcome::Failed;
    else if (waitResult != successCode)
        outcome = SubmissionOutcome::TimedOut;
    else
        outcome = SubmissionOutcome::Completed;

    if (outcome == SubmissionOutcome::Completed) {
        outReusable = std::move(reusable);                 // keep the buffer for Map/reuse
        return ReadbackDispatch::ProceedToMap;
    }
    if (!t.canSubmit()) {
        outReusable = std::move(reusable);                 // at cap: do not park a further buffer
        return ReadbackDispatch::Stalled;
    }
    rec.buffer = std::move(reusable);                      // RETENTION: buffer now owned by the parked record
    outReusable = Buffer{};                                // REPLACEMENT: caller's handle cleared
    t.parkIfUnresolved(std::move(rec), outcome);           // parked ALIVE (never reused/freed here)
    return outcome == SubmissionOutcome::Failed ? ReadbackDispatch::ParkedFailed
                                                : ReadbackDispatch::ParkedTimeout;
}

} // namespace omt
