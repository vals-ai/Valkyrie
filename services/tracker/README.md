### Required Environment variables

Create a `.env` file in the project root with the following variables:

```env
BENCHMARK_SERVICE_URL=http://98....
DAYTONA_API_KEY=...
DAYTONA_API_URL=https://app.daytona.io/api
DAYTONA_TARGET=us
```

### Docker Deployment

Build and run the tracker service in a Docker container:

```bash
# ----- Main command -----
# Clean, build and run the tracker service
make tracker-service

# ----- Helper commands -----
# Build the Docker image
make build

# Run the container (automatically loads .env file)
make run

# View container logs
make logs

# Stop the container
make stop

# Clean up (remove container and image)
make clean
```

The service will be available at `http://localhost:8000`

### Running unit tests

`uv run pytest tests/unit -vv`

### Database creation / Migration guide

View all documentation inside of the dedicated [README.md](src/tracker/database/README.md)