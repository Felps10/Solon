import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")

app = FastAPI(
    title="Sólon API",
    description="Temporal political knowledge system — Memória Política Brasileira",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


from app.api.people import router as people_router
app.include_router(people_router)

from app.routers import search as search_router
from app.routers import people as people_v1_router
from app.routers import candidacies as candidacies_router

app.include_router(search_router.router,       prefix="/api/v1")
app.include_router(people_v1_router.router,    prefix="/api/v1")
app.include_router(candidacies_router.router,  prefix="/api/v1")


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "solon-api",
        "version": "0.1.0",
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}
