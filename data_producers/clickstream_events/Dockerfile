FROM rust:slim-bullseye

ARG APP_NAME=clickstream_events
WORKDIR /app

# rdkafka requires cmake, pkg-config, and ssl/sasl headers to compile.
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    cmake \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Caching Layer
COPY Cargo.lock Cargo.toml ./
RUN mkdir src \
    && echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs \
    && cargo build --release

COPY src src
RUN touch src/main.rs 
RUN cargo build --locked --release

# Move binary
RUN cp ./target/release/$APP_NAME /bin/server

CMD ["/bin/server"]