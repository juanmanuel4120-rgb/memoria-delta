import base64, hashlib, os, time, asyncio
from typing import List
import boto3
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    S3_ENDPOINT: str
    S3_KEY: str
    S3_SECRET: str
    S3_BUCKET: str
    S3_REGION: str = "auto"
    PG_DSN: str
    model_config = SettingsConfigDict(env_file=".env")

cfg = Config()
SIZE = 131_072

s3 = boto3.client("s3", endpoint_url=cfg.S3_ENDPOINT, aws_access_key_id=cfg.S3_KEY, aws_secret_access_key=cfg.S3_SECRET, region_name=cfg.S3_REGION)
pg_pool = None

app = FastAPI(title="External Memory Service")

@app.on_event("startup")
async def startup():
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=cfg.PG_DSN, min_size=1, max_size=4)

class WriteIn(BaseModel):
    doc_id: str
    chunks_b64: List[str]
    mime: str = "text/plain"

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

@app.post("/write")
async def write_file(payload: WriteIn):
    # Usamos decodebytes para mayor compatibilidad
    chunks = [base64.decodebytes(c.encode()) for c in payload.chunks_b64]
    hashes = [sha256(c) for c in chunks]
    full = sha256(b"".join(chunks))
    for h, blob in zip(hashes, chunks):
        k = f"chunks/{h[:2]}/{h[2:4]}/{h}"
        try:
            s3.head_object(Bucket=cfg.S3_BUCKET, Key=k)
        except:
            s3.put_object(Bucket=cfg.S3_BUCKET, Key=k, Body=blob)
    async with pg_pool.acquire() as conn:
        ts = int(time.time())
        await conn.execute("INSERT INTO documents (id, mime, created, updated, fragments, full_hash) VALUES ($1,$2,$3,$3,$4,$5) ON CONFLICT (id) DO UPDATE SET updated=$3, fragments=$4, full_hash=$5", payload.doc_id, payload.mime, ts, hashes, full)
    return {"status": "ok", "doc_id": payload.doc_id}

@app.get("/read/{doc_id}")
async def read_file(doc_id: str):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT fragments, full_hash FROM documents WHERE id=$1", doc_id)
    if not row: raise HTTPException(404)
    hashes = row["fragments"]
    blobs = []
    for h in hashes:
        obj = s3.get_object(Bucket=cfg.S3_BUCKET, Key=f"chunks/{h[:2]}/{h[2:4]}/{h}")
        blobs.append(obj["Body"].read())
    return {"doc_id": doc_id, "text": b"".join(blobs).decode()}
