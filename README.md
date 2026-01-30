# DbAdmin AI 🤖

> **Natural language database administration powered by AI**

Ask questions in plain English, get SQL queries, optimization tips, and database insights. Supports PostgreSQL, MySQL, MongoDB, and Redis.

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## ✨ Features

### 🗣️ Natural Language Queries
```bash
dbadmin query "show me users who signed up this month" -d my-postgres
dbadmin query "find orders over $1000 with customer info" -d mysql://localhost/shop
```

### 🧠 Smart Task Routing
Automatically routes tasks to the right AI model:
- **Simple tasks** (80%) → Fast, cheap models (Llama 3.1 8B, GPT-4o-mini)
- **Complex tasks** (20%) → Capable models (Claude 3.5 Sonnet, GPT-4o)
- **~90% cost reduction** while maintaining quality

### ✅ SQL Verification (Critic Pattern)
Two-model verification catches errors before execution:
1. Generator model creates SQL
2. Different critic model reviews for errors
3. Auto-regenerates if issues found

### 🔌 Plug-and-Play AI Models
Works with any OpenAI-compatible API:

| Provider | Free Tier | Setup |
|----------|-----------|-------|
| OpenRouter | ✅ Many free models | `OPENROUTER_API_KEY` |
| Groq | ✅ Fast inference | `GROQ_API_KEY` |
| Ollama | ✅ Local, unlimited | Just run `ollama serve` |
| OpenAI | ❌ | `OPENAI_API_KEY` |
| Anthropic | ❌ | `ANTHROPIC_API_KEY` |

### 📊 Database Support
- **PostgreSQL** - Full support with pg_stat_statements
- **MySQL/MariaDB** - Query analysis, slow log parsing
- **MongoDB** - Collection inspection, aggregation help
- **Redis** - Key analysis, memory optimization

---

## 🚀 Quick Start

### Installation
```bash
git clone https://github.com/yourusername/dbadmin-ai.git
cd dbadmin-ai
python -m venv .venv
.venv/Scripts/activate  # Windows
# source .venv/bin/activate  # Linux/Mac
pip install -e .
```

### Setup (Pick One)
```bash
# Option 1: OpenRouter (recommended - free models available)
# Get key at https://openrouter.ai/keys
export OPENROUTER_API_KEY=sk-or-v1-...

# Option 2: Groq (free tier)
export GROQ_API_KEY=gsk_...

# Option 3: Local Ollama (free, unlimited)
ollama serve
```

### First Query
```bash
dbadmin query "show all tables" -d postgresql://localhost/mydb
```

---

## 📖 Commands

### `dbadmin chat` - Interactive Assistant
```bash
dbadmin chat -d postgresql://localhost/mydb

# Ask anything:
# > Why is my users query slow?
# > What indexes should I add?
# > Help me optimize this JOIN
```

### `dbadmin query` - Direct NL-to-SQL
```bash
dbadmin query "top 10 users by order count" -d my-db
dbadmin query "count orders by status this week" -d my-db --format json
```

### `dbadmin health` - Health Check
```bash
dbadmin health my-postgres
# Shows: Health score, connection stats, cache hit ratio, recommendations
```

### `dbadmin recommend` - Optimization Tips
```bash
dbadmin recommend my-db --type index
dbadmin recommend my-db --type query
```

### `dbadmin analyze` - Query Analysis
```bash
dbadmin analyze "SELECT * FROM users WHERE email LIKE '%@gmail.com'"
# Shows: Explain plan, performance issues, optimization suggestions
```

### `dbadmin connect` - Connection Management
```bash
dbadmin connect postgresql://user:pass@host:5432/db --save my-db
dbadmin connect --list
```

---

## ⚙️ Configuration

### Environment Variables
```bash
# AI Provider (pick one)
OPENROUTER_API_KEY=sk-or-...
GROQ_API_KEY=gsk_...
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...

# Or any OpenAI-compatible API
LLM_BASE_URL=https://your-api.com/v1
LLM_API_KEY=your-key
LLM_MODEL=your-model

# Database connections (optional, can also use URLs directly)
POSTGRES_URL=postgresql://localhost/mydb
MYSQL_URL=mysql://localhost/mydb
MONGODB_URL=mongodb://localhost/mydb
REDIS_URL=redis://localhost

# Storage
CHROMA_PERSIST_DIR=./data/chroma
```

### Saved Connections
```bash
# Save a connection
dbadmin connect postgresql://user:pass@host/db --save production

# Use by name
dbadmin query "show tables" -d production
```

---

## 🏗️ Architecture

```
src/dbadmin/
├── cli/                 # Typer CLI commands
│   └── commands/        # chat, query, health, analyze, recommend, connect
├── ai/
│   ├── llm.py          # Plug-and-play LLM client
│   ├── router.py       # Smart task routing + critic pattern
│   ├── chat.py         # Conversation session
│   └── prompts.py      # Prompt templates
├── connectors/          # Database connectors
│   ├── postgresql.py   # psycopg3
│   ├── mysql.py        # mysql-connector
│   ├── mongodb.py      # pymongo
│   └── redis.py        # redis-py
├── rag/                 # RAG with ChromaDB
│   ├── vectorstore.py  # Vector storage
│   ├── retriever.py    # Document retrieval
│   └── ingest.py       # Documentation ingestion
└── analysis/            # Query & health analysis
    ├── health.py       # Health scoring
    ├── query.py        # Query analysis
    └── index.py        # Index recommendations
```

---

## 🤝 Contributing
Contributions welcome! Please read our contributing guidelines.

## 📄 License
MIT License - see [LICENSE](LICENSE) for details.
