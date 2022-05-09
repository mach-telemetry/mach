FROM rust:latest

WORKDIR /mach
COPY ./ ./
RUN rustup override set nightly-2021-11-16
RUN rustup component add rustfmt --toolchain nightly-2021-11-16-x86_64-unknown-linux-gnu
RUN cargo build --release --bin tsdb
CMD cargo run --release --bin tsdb -- --kafka $KAFKA_BOOTSTRAP

