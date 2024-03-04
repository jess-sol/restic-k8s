FROM rust:1-alpine3.19 as build
RUN apk add --no-cache musl-dev
COPY . /app/
WORKDIR /app
RUN --mount=type=cache,target=/usr/local/cargo/git \
  --mount=type=cache,target=/usr/local/cargo/registry \
  cargo build -p walle --release

FROM alpine:latest
RUN sed -i '/^nobody:/d' /etc/passwd && \
  echo "nobody:x:65534:65534:nobody:/home/nobody:/bin/sh" >> /etc/passwd && \
  echo "nobody:x:65534:" >> /etc/group && \
  mkdir /home/nobody && \
  chown nobody:nobody /home/nobody
USER nobody:nobody
COPY --from=build /app/target/release/walle /
ENTRYPOINT ["/walle"]
