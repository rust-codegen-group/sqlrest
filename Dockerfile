# Package a verified Linux artifact, rather than downloading a compiler at build time.
# Build on Ubuntu 22.04 (glibc 2.35) or Debian 12 (glibc 2.36), matching image arch:
# cargo build --release --locked
FROM debian:bookworm-slim@sha256:4724b8cc51e33e398f0e2e15e18d5ec2851ff0c2280647e1310bc1642182655d
COPY target/release/sqlrest /usr/local/bin/sqlrest
COPY LICENSE-MIT LICENSE-APACHE /usr/share/doc/sqlrest/
USER 65532:65532
EXPOSE 8080 8081
ENTRYPOINT ["/usr/local/bin/sqlrest"]
# Deliberately no default bind addresses: the operator must choose both.
