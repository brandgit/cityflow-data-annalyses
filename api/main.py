"""
Point d'entrée pour lancer l'API FastAPI avec Uvicorn.
"""

from __future__ import annotations

import uvicorn


def run(host: str = "0.0.0.0", port: int = 8000) -> None:
    """
    Lance le serveur Uvicorn.
    """
    uvicorn.run("api.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    run()


