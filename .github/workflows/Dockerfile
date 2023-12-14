FROM alpine:latest AS build

RUN apk add alpine-sdk

RUN git clone -b development-v2 https://github.com/threefoldtech/0-db /zdb && \
  cd /zdb/libzdb && \
  make release && \
  cd ../zdbd && \
  make STATIC=1 release

FROM busybox:latest

COPY --from=build /zdb/zdbd/zdb /bin/zdb
COPY zdb-process /bin/

RUN chmod +x /bin/zdb-process

WORKDIR /

ENTRYPOINT ["/bin/zdb-process"]
