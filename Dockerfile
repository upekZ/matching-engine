FROM golang:1.23

WORKDIR /app

RUN go install github.com/air-verse/air@v1.61.0

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o matching-engine cmd/api/main.go

EXPOSE 3000 8080
CMD ["sh", "-c", "if [ \"$ENV\" = \"production\" ]; then ./matching-engine; else air -c .air.toml; fi"]