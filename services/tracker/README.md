### Required Environment variables

```
SWEBENCH_SERVICE_IP=13.2...

DAYTONA_API_KEY=...
DAYTONA_API_URL=https://app.daytona.io/api
DAYTONA_TARGET=us
```

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
