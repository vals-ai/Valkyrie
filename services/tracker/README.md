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

`uv run pytest tests/integration -vv`

### Creating session

`uv run src/tracker/database/session.py`

You should see the `tracker.db` located at `src/tracker/database/tracker.db`

### Adding table to database

Add the table with the option `table=True`

```python
# src/tracker/database/models.py

class Benchmark(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid4, primary_key=True)
    name: str
    started_at: datetime = Field(default_factory=datetime.now)
    finished_at: datetime | None = None
    ...
```

Import the table to expose it to SQLModel when you regenerate the tables

```python
# src/tracker/database/session.py

from src.tracker.database.models import Benchmark, EvaluationResult, Task

_exposed_models: list[type[SQLModel]] = [Benchmark, EvaluationResult, Task]
```

Regenerate the session `uv run src/tracker/database/session.py`
