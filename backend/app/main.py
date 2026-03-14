from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Sólon API",
    description="Temporal political knowledge system — Memória Política Brasileira",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


from app.api.people import router as people_router
app.include_router(people_router)


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
