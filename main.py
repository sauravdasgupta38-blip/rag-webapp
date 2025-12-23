from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"health": "ok"}
