### Required Environment variables

Create a `.env` file in the project root with the following variables:

```env
BENCHMARK_SERVICE_URL=https://benchmark-service.vals.ai
DAYTONA_API_KEY=...
DAYTONA_API_URL=https://app.daytona.io/api
DAYTONA_TARGET=us
```

### Docker Compose Environment

`make setup`: Install dependencies and create database (host machine database used in container)

`make tracker-service`: Build and run the tracker service in a Docker container

The service will be available at `http://localhost:8000`

### Running unit tests

`uv run pytest tests/unit -vv`

### Database creation / Migration guide

View all documentation inside of the dedicated [README.md](src/tracker/database/README.md)