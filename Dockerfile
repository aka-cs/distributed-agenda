FROM go-grpc

COPY . ./app

WORKDIR ./app/server

EXPOSE 3333:3333

RUN go env -w GOPROXY=http://nexus.prod.uci.cu/repository/proxy.golang.org,direct

RUN go mod tidy

CMD ["go", "run", "main/main.go"]