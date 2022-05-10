FROM rust:latest

WORKDIR /mach
COPY ./ ./
RUN rustup override set nightly-2021-11-16
RUN rustup component add rustfmt --toolchain nightly-2021-11-16-x86_64-unknown-linux-gnu
EXPOSE 50050
RUN cargo build --release --bin tsdb
CMD cargo run --release --bin tsdb -- --kafka "localhost:9093,localhost:9094,localhost:9095"

