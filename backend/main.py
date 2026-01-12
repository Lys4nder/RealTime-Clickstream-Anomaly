import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.routes import router as api_router

app = FastAPI()

# Enable CORS for Angular
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API router from src.routes
app.include_router(api_router)
