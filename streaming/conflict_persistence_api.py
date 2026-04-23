
from fastapi import FastAPI

app = FastAPI()

@app.get("/api/streaming/evidence")
async def list_evidence():
    return {"data": []}

@app.get("/api/streaming/resolved-assertions")
async def list_resolved():
    return {"data": []}

@app.get("/api/streaming/disputes")
async def list_disputes():
    return {"data": []}

@app.get("/api/streaming/topology/current")
async def current_topology():
    return {"graph": {}}
