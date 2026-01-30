# Contributing to DbAdmin AI

Thank you for your interest in contributing! 

## Development Setup

```bash
git clone https://github.com/yourusername/dbadmin-ai.git
cd dbadmin-ai
python -m venv .venv
.venv/Scripts/activate
pip install -e ".[dev]"
```

## Running Tests

```bash
pytest
pytest --cov=dbadmin  # with coverage
```

## Code Style

- We use `ruff` for linting and formatting
- Type hints are required for all functions
- Docstrings follow Google style

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit (`git commit -m 'Add amazing feature'`)
6. Push (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Areas for Contribution

- [ ] Additional database connectors (SQLite, Oracle, etc.)
- [ ] More LLM provider integrations
- [ ] Web dashboard (FastAPI + React)
- [ ] Documentation improvements
- [ ] Test coverage
