# Database

PostgreSQL database managed via SQLModel and Alembic.

## Adding a table

Add the table with `table=True`:

```python
# src/tracker/database/models.py

class Benchmark(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str
    started_at: datetime = Field(default_factory=datetime.now)
    finished_at: datetime | None = None
    ...
```

Import the table in `session.py` to expose it to SQLModel:

```python
# src/tracker/database/session.py

from tracker.database.models import Benchmark, FinalEvaluation, Task

_exposed_models: list[type[SQLModel]] = [Benchmark, FinalEvaluation, Task]
```

## Migrations

### Generate a new migration

From `services/tracker/`:

```bash
make migrate-gen
```

This runs `alembic revision --autogenerate` and generates a file in `src/tracker/database/migrations/versions/`.

### Apply migrations

```bash
uv run alembic upgrade head
```

## Alembic Tests

Installed via `pytest-alembic`, also run on push and pull:

```bash
make test-alembic
```

## Known Defects

### Custom TypeDecorator not being sourced on migration

Alembic may generate a migration referencing the type by module path:

```text
sa.Column('arguments', tracker.database.models.BenchmarkArgumentsType(), nullable=True),
                           ^^^^^^^
NameError: name 'tracker' is not defined
```

**Fix:** Import the type at the top of the generated migration file:

```python
from tracker.database.models import BenchmarkArgumentsType
```

Then update the reference in the migration to use the short name:

```python
sa.Column('arguments', BenchmarkArgumentsType(), nullable=True),
```

Run the upgrade: `uv run alembic upgrade head`
