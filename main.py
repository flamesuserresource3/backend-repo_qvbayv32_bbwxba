import os
import secrets
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from psycopg.types.json import Json

# ---------- Database Setup (Xata Postgres) ----------
XATA_PG_URL = os.getenv(
    "XATA_DATABASE_URL",
    "postgresql://uuq2nd:xau_9rvJrns2TvruhCrmSmELfE4SfJinxSl01@ap-southeast-2.sql.xata.sh/zlog:main?sslmode=require",
)

pool: ConnectionPool = ConnectionPool(conninfo=XATA_PG_URL, min_size=1, max_size=10, kwargs={"autocommit": False})

# Initialize tables
DDL_PROJECTS = """
CREATE TABLE IF NOT EXISTS projects (
  id        VARCHAR(36) PRIMARY KEY,
  name      VARCHAR(255) NOT NULL,
  api_key   VARCHAR(64) UNIQUE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

DDL_LOGS = """
CREATE TABLE IF NOT EXISTS logs (
  id SERIAL PRIMARY KEY,
  project_id VARCHAR(36) NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  service VARCHAR(100),
  framework VARCHAR(50),
  method VARCHAR(10),
  path VARCHAR(1024),
  status INT,
  duration_ms INT,
  ip VARCHAR(100),
  user_agent TEXT,
  request_body JSONB,
  response_body JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_logs_project ON logs(project_id);
CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);
"""

with pool.connection() as conn:
    with conn.cursor() as cur:
        cur.execute(DDL_PROJECTS)
        cur.execute(DDL_LOGS)
        conn.commit()

# ---------- FastAPI App ----------
app = FastAPI(title="ZLog - API Logger")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Schemas ----------
class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=2, max_length=255)

class ProjectOut(BaseModel):
    id: str
    name: str
    api_key: str
    created_at: datetime

class LogIngest(BaseModel):
    service: Optional[str] = None
    framework: Optional[str] = None
    method: Optional[str] = None
    path: Optional[str] = None
    status: Optional[int] = None
    duration_ms: Optional[int] = None
    ip: Optional[str] = None
    user_agent: Optional[str] = None
    request_body: Optional[dict] = None
    response_body: Optional[dict] = None

class LogOut(BaseModel):
    id: int
    project_id: str
    service: Optional[str]
    framework: Optional[str]
    method: Optional[str]
    path: Optional[str]
    status: Optional[int]
    duration_ms: Optional[int]
    ip: Optional[str]
    user_agent: Optional[str]
    request_body: Optional[dict]
    response_body: Optional[dict]
    created_at: datetime

# ---------- Helpers ----------

def generate_api_key() -> str:
    return secrets.token_hex(24)

def get_project_by_key(api_key: str) -> Optional[dict]:
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM projects WHERE api_key = %s LIMIT 1", (api_key,))
            row = cur.fetchone()
            return dict(row) if row else None

# ---------- Routes ----------
@app.get("/")
def root():
    return {"name": "ZLog", "status": "ok"}

@app.post("/api/projects", response_model=ProjectOut)
def create_project(payload: ProjectCreate):
    pid = secrets.token_hex(8)
    key = generate_api_key()
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "INSERT INTO projects (id, name, api_key) VALUES (%s, %s, %s) RETURNING id, name, api_key, created_at",
                (pid, payload.name, key),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                raise HTTPException(status_code=500, detail="Failed to create project")
            return ProjectOut(**row)

@app.get("/api/projects", response_model=List[ProjectOut])
def list_projects():
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT id, name, api_key, created_at FROM projects ORDER BY created_at DESC")
            rows = cur.fetchall() or []
            return [ProjectOut(**dict(r)) for r in rows]

@app.post("/ingest")
def ingest_log(request: Request, payload: LogIngest):
    api_key = request.headers.get("X-API-Key") or request.headers.get("x-api-key")
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")

    proj = get_project_by_key(api_key)
    if not proj:
        raise HTTPException(status_code=401, detail="Invalid API key")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO logs (
                  project_id, service, framework, method, path, status, duration_ms, ip, user_agent, request_body, response_body
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    proj["id"], payload.service, payload.framework, payload.method, payload.path,
                    payload.status, payload.duration_ms, payload.ip, payload.user_agent,
                    Json(payload.request_body) if payload.request_body is not None else None,
                    Json(payload.response_body) if payload.response_body is not None else None,
                ),
            )
            conn.commit()
    return {"ok": True}

@app.get("/api/logs", response_model=List[LogOut])
def get_logs(project_id: str, q: Optional[str] = None, limit: int = 100):
    if limit > 500:
        limit = 500
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if q:
                like = f"%{q}%"
                cur.execute(
                    """
                    SELECT * FROM logs
                    WHERE project_id = %s
                      AND (COALESCE(path,'') ILIKE %s OR COALESCE(method,'') ILIKE %s OR COALESCE(service,'') ILIKE %s OR COALESCE(framework,'') ILIKE %s)
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (project_id, like, like, like, like, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM logs WHERE project_id = %s ORDER BY id DESC LIMIT %s",
                    (project_id, limit),
                )
            rows = [dict(r) for r in (cur.fetchall() or [])]
            for r in rows:
                if isinstance(r.get("created_at"), datetime):
                    r["created_at"] = r["created_at"].isoformat()
            return [LogOut(**r) for r in rows]

@app.get("/api/stats")
def get_stats(project_id: str):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM logs WHERE project_id = %s", (project_id,))
            total = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*) FROM logs
                WHERE project_id = %s AND created_at > NOW() - INTERVAL '24 hours'
                """,
                (project_id,),
            )
            last_24h = cur.fetchone()[0]
            cur.execute(
                "SELECT status, COUNT(*) FROM logs WHERE project_id = %s GROUP BY status",
                (project_id,),
            )
            by_status_rows = cur.fetchall() or []
            by_status = {str(k if k is not None else "unknown"): int(v) for k, v in by_status_rows}
    return {"total": int(total or 0), "last_24h": int(last_24h or 0), "by_status": by_status}

@app.get("/api/snippets")
def snippets():
    express = """
// Express.js middleware
import axios from 'axios';

export function zlog(apiKey) {
  return async (req, res, next) => {
    const start = Date.now();
    res.on('finish', async () => {
      try {
        await axios.post(`${process.env.ZLOG_URL || 'https://your-backend-url'}/ingest`, {
          service: process.env.SERVICE_NAME || 'api',
          framework: 'express',
          method: req.method,
          path: req.originalUrl,
          status: res.statusCode,
          duration_ms: Date.now() - start,
          ip: req.ip,
          user_agent: req.headers['user-agent'],
          request_body: req.body,
        }, { headers: { 'X-API-Key': apiKey } });
      } catch (e) {}
    });
    next();
  };
}
"""

    hono = """
// Hono.js middleware
import { Hono } from 'hono'

export const zlog = (apiKey, baseUrl = (process.env.ZLOG_URL || 'https://your-backend-url')) => async (c, next) => {
  const start = Date.now();
  await next();
  try {
    await fetch(`${baseUrl}/ingest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': apiKey },
      body: JSON.stringify({
        service: (process.env.SERVICE_NAME || 'api'),
        framework: 'hono',
        method: c.req.method,
        path: c.req.path,
        status: c.res.status,
        duration_ms: Date.now() - start,
        ip: c.req.header('x-forwarded-for') || '',
        user_agent: c.req.header('user-agent') || ''
      })
    })
  } catch (e) {}
}
"""

    return { express, hono }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
