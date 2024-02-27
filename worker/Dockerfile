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
RUN sed -i '/^nobody:/d' /etc/passwd && \
  echo "nobody:x:65534:65534:nobody:/home/nobody:/bin/sh" >> /etc/passwd && \
  echo "nobody:x:65534:" >> /etc/group && \
  mkdir /home/nobody && \
  chown nobody:nobody /home/nobody
USER nobody:nobody
COPY --from=restic-build /go/src/restic/restic /
COPY --from=build /app/target/release/walle-worker /
ENTRYPOINT ["/walle-worker"]
