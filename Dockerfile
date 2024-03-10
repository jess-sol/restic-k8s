FROM rust:1 as build
COPY . /app/
WORKDIR /app
RUN --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/usr/local/cargo/registry \
  cargo build -p walle --release

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /app/target/release/walle /app/target/release/crdgen /
ENTRYPOINT ["/walle"]
