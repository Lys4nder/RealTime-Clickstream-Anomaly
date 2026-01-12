from fastapi import APIRouter

router = APIRouter()

from .batch import router as batch_router  # noqa: E402
from .realtime import router as realtime_router  # noqa: E402

router.include_router(batch_router, prefix="/api")
router.include_router(realtime_router, prefix="/api")
