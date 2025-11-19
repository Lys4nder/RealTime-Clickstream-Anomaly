FROM rust:1.91-slim-bullseye
ARG APP_NAME=clickstream_events

WORKDIR /app

COPY Cargo.lock Cargo.toml ./
RUN mkdir src \
    && echo "// dummy file" > src/lib.rs \
    && cargo build --release

COPY src src
RUN cargo build --locked --release
RUN cp ./target/release/$APP_NAME /bin/server

ENV ROCKET_ADDRESS=0.0.0.0
CMD ["/bin/server"]