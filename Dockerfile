# build frontend --------------------------
FROM node:16-alpine as frontend
WORKDIR /frontend

COPY frontend /frontend/
RUN npm install
RUN npm run build

# build webserver and bapiserver --------------------------
FROM gcr.io/cloud-builders/bazel as service
WORKDIR /service

COPY service /service/
RUN bazel build --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 //:binary

# build image for deployment --------------------------
FROM golang:alpine3.15 as app
WORKDIR /app

RUN apk add dumb-init

# copy over static site
RUN mkdir static
COPY --from=frontend /frontend/dist /app/static/
# copy over bapiserver, webserver, and script to run the service
COPY --from=service /service/bazel-bin/cmd/bapiserver/bapiserver_/bapiserver /app/
COPY --from=service /service/bazel-bin/cmd/webserver/webserver_/webserver /app/
COPY run_service /app/

ENTRYPOINT ["dumb-init"]
CMD ["./run_service"]

USER nobody
