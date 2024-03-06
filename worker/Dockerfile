FROM golang:1 as restic-build

RUN git clone https://github.com/restic/restic /go/src/restic
WORKDIR /go/src/restic
RUN git checkout v0.16.4

RUN go run build.go

FROM rust:1 as build
COPY . /app/
WORKDIR /app
RUN --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/usr/local/cargo/registry \
  cargo build -p walle-worker --release

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=restic-build /go/src/restic/restic /
COPY --from=build /app/target/release/walle-worker /
ENTRYPOINT ["/walle-worker"]
