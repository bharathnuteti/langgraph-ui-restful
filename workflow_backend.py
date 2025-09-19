# workflow_backend.py
from test import Engine, InMemoryStore

# One store + engine for the whole process
store = InMemoryStore()
engine = Engine(store)