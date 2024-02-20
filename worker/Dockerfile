FROM golang:1-alpine as restic-build

RUN apk add git
RUN git clone https://github.com/restic/restic /go/src/restic
WORKDIR /go/src/restic
RUN git checkout v0.16.4

RUN go run build.go

FROM rust:1-alpine3.19 as build
RUN apk add --no-cache musl-dev
COPY . /app/
WORKDIR /app
RUN --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/usr/local/cargo/registry \
  cargo build -p walle-worker --release

FROM alpine:latest
RUN echo "nobody:x:1:1:nobody:/:/bin/sh" >> /etc/passwd && \
  echo "nobody:x:1:" >> /etc/group
USER nobody:nobody
COPY --from=restic-build /go/src/restic/restic /
COPY --from=build /app/target/release/walle-worker /
ENTRYPOINT ["/walle-worker"]
