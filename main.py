from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import json
from datetime import datetime
import os

DATABASE_URL = "sqlite:///./arima.db"  # Используем SQLite для простоты (не нужен PostgreSQL)
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

app = FastAPI(title="Arima Messenger")

# Модели
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    phone = Column(String(20), unique=True, index=True)
    first_name = Column(String(100))
    password_hash = Column(String(200))
    stars_balance = Column(Integer, default=10)
    is_online = Column(Boolean, default=False)

class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True)
    chat_id = Column(Integer)
    sender_id = Column(Integer)
    text = Column(Text, nullable=True)
    is_star_gift = Column(Boolean, default=False)
    star_cost = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def root():
    return {"message": "Arima Messenger API", "status": "running"}

@app.post("/register")
def register(phone: str, first_name: str, password: str, db: Session = Depends(get_db)):
    user = User(phone=phone, first_name=first_name, password_hash=password, stars_balance=10)
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"id": user.id, "stars": user.stars_balance, "message": "Registered!"}

@app.get("/users/{user_id}")
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    return {"id": user.id, "name": user.first_name, "stars": user.stars_balance}

@app.post("/buy_stars")
def buy_stars(user_id: int, amount: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    user.stars_balance += amount
    db.commit()
    return {"new_balance": user.stars_balance}

@app.post("/send_stars")
def send_stars(from_user: int, to_user: int, amount: int, db: Session = Depends(get_db)):
    sender = db.query(User).filter(User.id == from_user).first()
    receiver = db.query(User).filter(User.id == to_user).first()
    if not sender or not receiver:
        raise HTTPException(404, "User not found")
    if sender.stars_balance < amount:
        raise HTTPException(400, "Not enough stars")
    sender.stars_balance -= amount
    receiver.stars_balance += amount
    db.commit()
    return {"from_balance": sender.stars_balance, "to_balance": receiver.stars_balance}

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        print(f"User {user_id} disconnected")
