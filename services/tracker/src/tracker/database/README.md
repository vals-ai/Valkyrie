## Database

### Creating session

Only need to generate the session if it is your first time / tracker.db does not exist

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

### Migration Usage

#### Create new migration

File is generated inside of `src/tracker/database/migrations/versions`

`uv run -m alembic revision --autogenerate -m "Initial Migration"`

Change the database to use the latest changes

`uv run -m alembic upgrade head`

### Run Alembic Tests

These tests are installed via `pytest-alembic`, these will also be ran on push and pull.

`uv run pytest --test-alembic -m alembic`

### Known Defects

#### Custom TypeDecorator not being sourced on migration

Defect In Alembic

```text
sa.Column('arguments', tracker.database.models.BenchmarkArgumentsType(), nullable=True),
                           ^^^^^^^
NameError: name 'tracker' is not defined
```

#### Fix

Import the type at the top

`from tracker.database.models import BenchmarkArgumentsType`

Update reference

`sa.Column('arguments', BenchmarkArgumentsType(), nullable=True),`

Run the upgrade command `uv run -m alembic upgrade head`