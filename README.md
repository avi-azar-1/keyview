# KeyView

A dockerized web tool for inspecting Redis key distribution. Connect to any Redis instance and get interactive visualizations of key types, TTL distribution, and pattern-based grouping.

## Quick Start

```bash
docker-compose -f docker-compose.dev.yml up --build
```

This starts KeyView on http://localhost:8000 with a local Redis instance for testing.

To seed the dev Redis with sample data:

```bash
docker-compose -f docker-compose.dev.yml exec keyview python /app/scripts/seed-redis.py
```

## Production

```bash
docker-compose up --build
```

Then open http://localhost:8000 and enter your Redis connection details.

## Features

- Connect to any Redis instance (host, port, username, password)
- Scan keyspace with real-time progress (WebSocket)
- View key type distribution (donut chart)
- View TTL distribution (histogram)
- Define glob patterns to group keys (e.g., `user:*`, `session:*`)
- Auto-detect namespaces by `:` delimiter (treemap)
- Dark/light theme toggle

## Tech Stack

- **Backend**: Python 3.12, FastAPI, redis-py (async)
- **Frontend**: React 18, TypeScript, Vite, Tailwind CSS, Apache ECharts
- **Docker**: Multi-stage build, single container

## Development

### Backend

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Vite dev server proxies `/api` and `/ws` to the backend on port 8000.
