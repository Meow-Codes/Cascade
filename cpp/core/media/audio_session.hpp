#pragma once
// Cascade :: core::media :: AudioSender / AudioReceiver
//
// Ties the pieces above to Phase 2's UdpSocket. prepare_frame()/transmit()
// are deliberately separate calls (rather than one send_frame() doing
// both) so tests can simulate network packet loss honestly -- by calling
// prepare_frame() (which assigns a sequence and caches it for potential
// retransmit) but skipping transmit() -- instead of faking loss inside
// the sender, which would test the fake rather than the real recovery path.

#include <cstdint>
#include <string>
#include <vector>

#include "media/audio_packet.hpp"
#include "media/jitter_buffer.hpp"
#include "media/retransmit_cache.hpp"
#include "media/silence_detector.hpp"
#include "net/udp_socket.hpp"

namespace cascade::core::media {

class AudioSender {
public:
    AudioSender(std::string peer_ip, std::uint16_t peer_port, std::uint32_t frame_interval_ms,
                std::uint16_t bind_port = 0)
        : socket_(bind_port != 0 ? "127.0.0.1" : "", bind_port),
          peer_ip_(std::move(peer_ip)), peer_port_(peer_port), frame_interval_ms_(frame_interval_ms) {}

    // Builds the wire frame, assigns the next sequence, and caches it for
    // retransmit -- but does NOT send it. Caller decides whether to call
    // transmit() (simulating real delivery or, in tests, simulating loss
    // by simply not calling it).
    std::vector<std::uint8_t> prepare_frame(const std::vector<std::uint8_t>& payload) {
        bool silent = false;
        if (payload.size() >= 2) {
            silent = is_silence(reinterpret_cast<const std::int16_t*>(payload.data()), payload.size() / 2);
        }
        std::uint8_t flags = silent ? kFlagSilence : 0;
        std::uint32_t timestamp = sequence_ * frame_interval_ms_;
        auto bytes = encode_audio_packet(sequence_, timestamp, flags, payload.data(), payload.size());
        cache_.store(sequence_, bytes);
        sequence_++;
        return bytes;
    }

    void transmit(const std::vector<std::uint8_t>& frame_bytes) {
        socket_.send_to(frame_bytes.data(), frame_bytes.size(), peer_ip_, peer_port_);
    }

    void send_frame(const std::vector<std::uint8_t>& payload) { transmit(prepare_frame(payload)); }

    // Call periodically to drain any NACKs the receiver has sent us and
    // retransmit the cached frame if we still have it.
    void poll_and_handle_nacks() {
        while (auto pkt = socket_.try_recv()) {
            auto decoded = decode_packet(pkt->data.data(), pkt->data.size());
            if (decoded.kind == PacketKind::Nack) {
                auto cached = cache_.get(decoded.sequence);
                if (cached) socket_.send_to(cached->data(), cached->size(), peer_ip_, peer_port_);
            }
        }
    }

    net::UdpSocket& socket() { return socket_; }

private:
    net::UdpSocket socket_;
    std::string peer_ip_;
    std::uint16_t peer_port_;
    std::uint32_t frame_interval_ms_;
    std::uint32_t sequence_ = 0;
    RetransmitCache cache_;
};

class AudioReceiver {
public:
    AudioReceiver(const std::string& bind_ip, std::uint16_t bind_port, std::string sender_ip,
                  std::uint16_t sender_port, std::uint32_t frame_interval_ms, std::uint32_t target_delay_ms)
        : socket_(bind_ip, bind_port), sender_ip_(std::move(sender_ip)), sender_port_(sender_port),
          jitter_(frame_interval_ms, target_delay_ms) {}

    // Drains any datagrams currently available into the jitter buffer.
    // now_ms is the receiver's own clock reading at call time.
    void poll_incoming(std::uint64_t now_ms) {
        while (auto pkt = socket_.try_recv()) {
            auto decoded = decode_packet(pkt->data.data(), pkt->data.size());
            if (decoded.kind == PacketKind::Audio) {
                jitter_.push(decoded.sequence, decoded.payload, now_ms);
            }
            // Nack decode isn't expected here -- receivers don't receive
            // NACKs, they send them. A stray one is simply ignored.
        }
    }

    void request_retransmit(std::uint32_t sequence) {
        auto bytes = encode_nack(sequence);
        socket_.send_to(bytes.data(), bytes.size(), sender_ip_, sender_port_);
    }

    JitterBuffer::PullResult pull(std::uint64_t now_ms) { return jitter_.pull(now_ms); }
    JitterBuffer& jitter_buffer() { return jitter_; }

private:
    net::UdpSocket socket_;
    std::string sender_ip_;
    std::uint16_t sender_port_;
    JitterBuffer jitter_;
};

} // namespace cascade::core::media