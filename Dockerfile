# FROM node:16-alpine as frontend

# WORKDIR /frontend

# COPY frontend/package.json frontend/package-lock.json /frontend/
# RUN npm install

# COPY frontend /frontend/
# RUN npm run build

FROM gcr.io/cloud-builders/bazel as service
WORKDIR /service
COPY service /service/
# COPY --from=frontend /frontend/dist /service/cmd/webserver/static/
RUN bazel build --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 //:binary

FROM golang:alpine3.15 as app

WORKDIR /app
RUN apk add dumb-init

COPY --from=service /service/bazel-bin/cmd/bapiserver/bapiserver_/bapiserver /app/
COPY --from=service /service/bazel-bin/cmd/webserver/webserver_/webserver /app/

COPY run_service /app/
ENTRYPOINT ["dumb-init"]
CMD ["./run_service"]

USER nobody
