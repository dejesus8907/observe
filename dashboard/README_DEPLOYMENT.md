# NetObserv Dashboard Deployment

## Environment
Set the frontend API base URL through:

- `VITE_NETOBSERV_API_BASE_URL`

Example:
```bash
VITE_NETOBSERV_API_BASE_URL=http://localhost:8000/api/streaming
```

## Local development
```bash
cd dashboard
npm install
cp .env.example .env
npm run dev
```

## Production build
```bash
cd dashboard
npm install
npm run build
```

The build output is emitted to the Vite default `dist/` directory.

## Deployment options

### Option A — separate frontend service
Serve `dashboard/dist/` from a static host or CDN and point it at the backend API.

Requirements:
- backend reachable from the browser
- CORS configured if origin differs
- auth path aligned with frontend deployment

### Option B — backend-served static files
Build the dashboard and copy the generated `dist/` folder into the backend static-serving path behind a reverse proxy.

Recommended reverse proxy behavior:
- `/api/*` -> backend API
- `/` and static assets -> dashboard build output

## Smoke test
A valid deployment should confirm:
- dashboard page loads
- `/api/streaming/disputes` reachable
- `/api/streaming/correlation/clusters` reachable
- `/api/streaming/topology/current` reachable
