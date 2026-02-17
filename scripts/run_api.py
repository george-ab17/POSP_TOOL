from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app import app


if __name__ == "__main__":
    import uvicorn
    from backend.config import API_HOST, API_PORT

    uvicorn.run("backend.app:app", host=API_HOST, port=API_PORT, reload=True)
