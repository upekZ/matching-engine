
FROM golang:1.21

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o matching-engine ./main.go

EXPOSE 3000 8080

CMD ["./matching-engine"]