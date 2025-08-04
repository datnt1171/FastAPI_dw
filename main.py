from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import logging

from app.core.config import settings
from app.core.auth import has_permission
from app.core.database import db_manager
from app.api.routes.crm import crm
# from app.api.routes import warehouse, color_mixing

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup - Initialize database pool
    await db_manager.init_pool()
    logger.info("FastAPI Data Warehouse Backend started")
    yield
    # Shutdown - Close database pool
    await db_manager.close_pool()
    logger.info("FastAPI Data Warehouse Backend stopped")

app = FastAPI(
    title="Data Warehouse Read-Only API",
    description="FastAPI backend for read-only data warehouse access with JWT authentication",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],  # Only GET requests allowed
    allow_headers=["*"],
)

# Include read-only routes
app.include_router(crm.router, prefix="/api/crm", tags=["crm"])
# app.include_router(warehouse.router, prefix="/api/warehouse", tags=["warehouse"])
# app.include_router(color_mixing.router, prefix="/api/color-mixing", tags=["color-mixing"])

@app.get("/")
async def root():
    return {"message": "Data Warehouse Read-Only API", "version": "1.0.0"}

@app.get("/health/")
async def health_check(permitted = Depends(has_permission())):
    return {"status": "healthy", "service": "data-warehouse-readonly-api"}
    
@app.get("/api/sales/")
async def get_sales_data(permitted = Depends(has_permission("read.dashboard.sales"))):
    return {"data": "sales data"}

@app.get("/api/wh-overall/") 
async def get_wh_overall(permitted = Depends(has_permission("read.dashboard.wh_overall1"))):
    return {"data": "warehouse data"}    

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )