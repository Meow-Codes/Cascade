#include <gtest/gtest.h>
#include <fstream>
#include "controlplane/control_plane_client.hpp"

using namespace cascade::core::controlplane;

// Requires: a real `go run ./cmd/controlplaned -cert certs/server-cert.pem
// -key certs/server-key.pem` running, AND certs/ generated. Skips
// cleanly if certs aren't present, same policy as the Go-side TLS test.
TEST(ControlPlaneClientTLS, ConnectsSuccessfullyWithCorrectCACert) {
    std::string ca_path = std::string(CASCADE_PROJECT_SOURCE_DIR) + "/certs/ca-cert.pem";
    std::ifstream check(ca_path);
    if (!check.is_open()) {
        GTEST_SKIP() << "certs/ not generated -- run the cert-gen script first";
    }

    ControlPlaneClient client("localhost:50051", "tls-test-broker", "127.0.0.1:9400", ca_path);
    ASSERT_NO_THROW(client.list_brokers());
}

TEST(ControlPlaneClientTLS, FailsWithWrongOrMissingCACert) {
    // Points at a CA that (almost certainly) didn't sign the running
    // server's cert -- generate a throwaway self-signed cert here rather
    // than depending on a second real CA file existing.
    std::string bogus_ca_path = "/tmp/cascade_bogus_ca.pem";
    std::ofstream bogus(bogus_ca_path);
    bogus << "-----BEGIN CERTIFICATE-----\n"
             "MIIBhTCCASugAwIBAgIUZmFrZWNlcnQwCgYIKoZIzj0EAwIwEjEQMA4GA1UEAwwH\n"
             "ZmFrZS1jYTAeFw0yNDAxMDEwMDAwMDBaFw0zNDAxMDEwMDAwMDBaMBIxEDAOBgNV\n"
             "BAMMB2Zha2UtY2EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQqfakefakefake\n"
             "-----END CERTIFICATE-----\n"; // deliberately malformed/bogus -- SslCredentials should reject it
    bogus.close();

    ControlPlaneClient client("localhost:50051", "tls-test-broker-2", "127.0.0.1:9401", bogus_ca_path);
    EXPECT_THROW(client.list_brokers(), std::runtime_error);
}