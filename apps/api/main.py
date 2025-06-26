from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apps.api.config import get_settings
from apps.api.routes.health import router as health_router
from apps.api.routes.leads import router as leads_router

settings = get_settings()

app = FastAPI(
    title=settings.api_name,
    version=settings.api_version,
    docs_url="/docs",      # Swagger
    redoc_url="/redoc",    # Redoc
)

# CORS – libere só os domínios necessários depois
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar rotas
app.include_router(health_router, prefix="/v1")
app.include_router(leads_router, prefix="/v1")
