# Use official Go image
FROM golang:1.24

# Set the working directory inside container
WORKDIR /app

# Copy go.mod and go.sum first (better Docker caching)
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the Go app from app/main.go
RUN go build -o redis_clone ./app/main.go

# Expose Redis port
EXPOSE 6379

# Run the compiled binary
CMD ["./redis_clone"]
