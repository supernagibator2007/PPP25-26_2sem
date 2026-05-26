# Для запуска из корневой папки выполнить команду:
# uvicorn main:app --reload
from fastapi import FastAPI

from etl import run_etl
from routes import posts


app = FastAPI()

app.include_router(posts.router)


@app.get("/")
def read_root():
    return {"info": "Go to /docs or /redoc for API documentation"}
