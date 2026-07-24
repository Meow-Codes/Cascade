#pragma once
// Cascade :: core::media :: BitrateController
//
// Selects a discrete quality level from a fixed ladder. Asymmetric
// response, matching real ABR systems and TCP congestion control:
// downgrade immediately on ANY bad reading (avoid stalls -- correctness
// over smoothness), but require several consecutive good readings before
// upgrading (avoid oscillation/thrashing, which hurts perceived quality
// more than staying one level lower).

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "media/network_estimator.hpp"

namespace cascade::core::media {

struct QualityLevel {
    std::string name;
    std::uint32_t bitrate_kbps;
};

class BitrateController {
public:
    // ladder MUST be sorted ascending by bitrate_kbps.
    // loss_threshold: at/above this loss rate, downgrade regardless of bandwidth.
    // upgrade_confirmations: consecutive good evaluate() calls required to move up.
    // bandwidth_headroom: require this much margin above a level's bitrate before selecting it.
    BitrateController(NetworkConditionEstimator& estimator, std::vector<QualityLevel> ladder,
                       double loss_threshold = 0.05, int upgrade_confirmations = 3,
                       double bandwidth_headroom = 1.3)
        : estimator_(estimator), ladder_(std::move(ladder)), loss_threshold_(loss_threshold),
          upgrade_confirmations_(upgrade_confirmations), bandwidth_headroom_(bandwidth_headroom),
          current_index_(0) {}

    const QualityLevel& evaluate(std::uint64_t now_ms) {
        std::lock_guard<std::mutex> lock(mutex_);
        double bw = estimator_.estimated_bandwidth_kbps(now_ms);
        double loss = estimator_.estimated_loss_rate();

        bool bad_conditions = (loss >= loss_threshold_) ||
                               (bw > 0.0 && bw < ladder_[current_index_].bitrate_kbps * bandwidth_headroom_);

        if (bad_conditions) {
            consecutive_good_ = 0;
            if (current_index_ > 0) current_index_--;
        } else {
            consecutive_good_++;
            if (consecutive_good_ >= upgrade_confirmations_ &&
                current_index_ + 1 < static_cast<int>(ladder_.size()) &&
                bw >= ladder_[current_index_ + 1].bitrate_kbps * bandwidth_headroom_) {
                current_index_++;
                consecutive_good_ = 0;
            }
        }
        return ladder_[current_index_];
    }

    const QualityLevel& current() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ladder_[current_index_];
    }

private:
    NetworkConditionEstimator& estimator_;
    std::vector<QualityLevel> ladder_;
    double loss_threshold_;
    int upgrade_confirmations_;
    double bandwidth_headroom_;

    int current_index_;
    int consecutive_good_ = 0;
    mutable std::mutex mutex_;
};

} // namespace cascade::core::media