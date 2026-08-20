from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, UploadFile, File, Form, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey, Text, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
from typing import Dict, Set, Optional
import os
import json
import aiofiles
import asyncio
from passlib.context import CryptContext

# --- ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/arima")

# --- БАЗА ДАННЫХ ---
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# --- ХЭШИРОВАНИЕ ПАРОЛЕЙ ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- МОДЕЛИ БАЗЫ ДАННЫХ ---
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    phone = Column(String(20), unique=True, index=True)
    username = Column(String(50), unique=True, nullable=True)
    first_name = Column(String(100))
    last_name = Column(String(100), nullable=True)
    password_hash = Column(String(200))
    avatar = Column(String(500), nullable=True)
    is_online = Column(Boolean, default=False)
    last_seen = Column(DateTime, default=datetime.utcnow)
    stars_balance = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class Chat(Base):
    __tablename__ = "chats"
    id = Column(Integer, primary_key=True)
    type = Column(String(20))  # "private", "group"
    name = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class ChatMember(Base):
    __tablename__ = "chat_members"
    id = Column(Integer, primary_key=True)
    chat_id = Column(Integer, ForeignKey("chats.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    role = Column(String(20), default="member")  # member, admin, creator

class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True)
    chat_id = Column(Integer, ForeignKey("chats.id"))
    sender_id = Column(Integer, ForeignKey("users.id"))
    text = Column(Text, nullable=True)
    file_path = Column(String(500), nullable=True)
    file_name = Column(String(255), nullable=True)
    file_size = Column(Integer, nullable=True)
    is_star_gift = Column(Boolean, default=False)
    star_cost = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class StarTransaction(Base):
    __tablename__ = "star_transactions"
    id = Column(Integer, primary_key=True)
    from_user_id = Column(Integer, ForeignKey("users.id"))
    to_user_id = Column(Integer, ForeignKey("users.id"))
    amount = Column(Integer)
    reason = Column(String(50))  # "gift", "purchase", "bonus", "transfer"
    created_at = Column(DateTime, default=datetime.utcnow)

# --- СОЗДАНИЕ ТАБЛИЦ ---
Base.metadata.create_all(bind=engine)

# --- FASTAPI ПРИЛОЖЕНИЕ ---
app = FastAPI(title="Арима Messenger", description="Полноценный мессенджер со звёздами", version="1.0.0")

# --- CORS (для доступа с телефона) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ЗАВИСИМОСТЬ ДЛЯ БД ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- ФУНКЦИИ ХЭШИРОВАНИЯ ---
def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

# --- WEBSOCKET МЕНЕДЖЕР ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, Set[WebSocket]] = {}
        self.typing_users: Dict[int, Set[int]] = {}
    
    async def connect(self, user_id: int, websocket: WebSocket):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
        self.active_connections[user_id].add(websocket)
        
        # Обновить статус в БД
        db = next(get_db())
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            user.is_online = True
            user.last_seen = datetime.utcnow()
            db.commit()
        db.close()
    
    def disconnect(self, user_id: int, websocket: WebSocket):
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
                
                # Обновить статус в БД
                db = next(get_db())
                user = db.query(User).filter(User.id == user_id).first()
                if user:
                    user.is_online = False
                    user.last_seen = datetime.utcnow()
                    db.commit()
                db.close()
    
    async def send_personal(self, user_id: int, data: dict):
        if user_id in self.active_connections:
            for ws in self.active_connections[user_id]:
                try:
                    await ws.send_json(data)
                except:
                    pass
    
    async def broadcast_to_chat(self, chat_id: int, data: dict, db: Session, exclude_user_id: int = None):
        members = db.query(ChatMember).filter(ChatMember.chat_id == chat_id).all()
        for member in members:
            if member.user_id != exclude_user_id:
                await self.send_personal(member.user_id, data)

manager = ConnectionManager()

# --- API ЭНДПОИНТЫ ---

# 1. РЕГИСТРАЦИЯ
@app.post("/register")
async def register(
    phone: str,
    first_name: str,
    password: str,
    last_name: Optional[str] = None,
    db: Session = Depends(get_db)
):
    # Проверка существующего пользователя
    existing = db.query(User).filter(User.phone == phone).first()
    if existing:
        raise HTTPException(400, "Phone already registered")
    
    # Создание пользователя
    user = User(
        phone=phone,
        first_name=first_name,
        last_name=last_name,
        password_hash=hash_password(password),
        stars_balance=10  # Бонус за регистрацию
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    
    # Создаём общий чат если его нет
    general_chat = db.query(Chat).filter(Chat.name == "Общий чат").first()
    if not general_chat:
        general_chat = Chat(type="group", name="Общий чат")
        db.add(general_chat)
        db.commit()
        db.refresh(general_chat)
    
    # Добавляем пользователя в общий чат
    member = ChatMember(chat_id=general_chat.id, user_id=user.id, role="member")
    db.add(member)
    db.commit()
    
    return {
        "id": user.id,
        "stars": user.stars_balance,
        "message": "Registration successful! 10 stars bonus!"
    }

# 2. ВХОД
@app.post("/login")
async def login(phone: str, password: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.phone == phone).first()
    if not user:
        raise HTTPException(404, "User not found")
    
    if not verify_password(password, user.password_hash):
        raise HTTPException(401, "Invalid password")
    
    return {
        "id": user.id,
        "first_name": user.first_name,
        "stars": user.stars_balance
    }

# 3. БАЛАНС ЗВЁЗД
@app.get("/balance/{user_id}")
async def get_balance(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    return {"stars": user.stars_balance}

# 4. ПОКУПКА ЗВЁЗД
@app.post("/buy_stars")
async def buy_stars(user_id: int, amount: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    
    if amount <= 0:
        raise HTTPException(400, "Amount must be positive")
    
    user.stars_balance += amount
    db.commit()
    
    return {"new_balance": user.stars_balance}

# 5. ПЕРЕВОД ЗВЁЗД
@app.post("/transfer_stars")
async def transfer_stars(from_user: int, to_user: int, amount: int, db: Session = Depends(get_db)):
    if amount <= 0:
        raise HTTPException(400, "Amount must be positive")
    
    sender = db.query(User).filter(User.id == from_user).first()
    receiver = db.query(User).filter(User.id == to_user).first()
    
    if not sender or not receiver:
        raise HTTPException(404, "User not found")
    
    if sender.stars_balance < amount:
        raise HTTPException(400, "Not enough stars")
    
    sender.stars_balance -= amount
    receiver.stars_balance += amount
    
    tx = StarTransaction(
        from_user_id=from_user,
        to_user_id=to_user,
        amount=amount,
        reason="transfer"
    )
    db.add(tx)
    db.commit()
    
    return {
        "from_balance": sender.stars_balance,
        "to_balance": receiver.stars_balance
    }

# 6. ОТПРАВКА СООБЩЕНИЯ
@app.post("/send_message")
async def send_message(
    chat_id: int,
    sender_id: int,
    text: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    is_star_gift: bool = Form(False),
    star_cost: int = Form(0),
    db: Session = Depends(get_db)
):
    # Проверка пользователя
    sender = db.query(User).filter(User.id == sender_id).first()
    if not sender:
        raise HTTPException(404, "Sender not found")
    
    # Проверка членства в чате
    member = db.query(ChatMember).filter(
        ChatMember.chat_id == chat_id,
        ChatMember.user_id == sender_id
    ).first()
    if not member:
        raise HTTPException(403, "Not a member of this chat")
    
    # Обработка звёздного подарка
    if is_star_gift and star_cost > 0:
        if sender.stars_balance < star_cost:
            raise HTTPException(400, "Not enough stars")
        sender.stars_balance -= star_cost
        
        # Находим получателя (первого в чате кроме отправителя)
        receiver_member = db.query(ChatMember).filter(
            ChatMember.chat_id == chat_id,
            ChatMember.user_id != sender_id
        ).first()
        
        if receiver_member:
            receiver = db.query(User).filter(User.id == receiver_member.user_id).first()
            if receiver:
                receiver.stars_balance += star_cost
                tx = StarTransaction(
                    from_user_id=sender_id,
                    to_user_id=receiver.id,
                    amount=star_cost,
                    reason="gift"
                )
                db.add(tx)
    
    # Обработка файла
    file_path = None
    file_name = None
    file_size = None
    
    if file:
        if file.size > 50 * 1024 * 1024:  # 50 MB
            raise HTTPException(400, "File too large (max 50MB)")
        
        os.makedirs("uploads", exist_ok=True)
        file_name = file.filename
        file_path = f"uploads/{chat_id}_{datetime.utcnow().timestamp()}_{file.filename}"
        
        async with aiofiles.open(file_path, "wb") as f:
            content = await file.read()
            await f.write(content)
        file_size = len(content)
    
    # Сохраняем сообщение
    msg = Message(
        chat_id=chat_id,
        sender_id=sender_id,
        text=text or "",
        file_path=file_path,
        file_name=file_name,
        file_size=file_size,
        is_star_gift=is_star_gift,
        star_cost=star_cost
    )
    db.add(msg)
    db.commit()
    db.refresh(msg)
    
    # Отправляем через WebSocket
    await manager.broadcast_to_chat(
        chat_id,
        {
            "type": "new_message",
            "message": {
                "id": msg.id,
                "sender_id": msg.sender_id,
                "text": msg.text,
                "file_path": msg.file_path,
                "file_name": msg.file_name,
                "is_star_gift": msg.is_star_gift,
                "star_cost": msg.star_cost,
                "created_at": msg.created_at.isoformat()
            }
        },
        db
    )
    
    return {
        "ok": True,
        "stars_left": sender.stars_balance,
        "message_id": msg.id
    }

# 7. ПОЛУЧИТЬ СООБЩЕНИЯ ЧАТА
@app.get("/messages/{chat_id}")
async def get_messages(chat_id: int, limit: int = 50, db: Session = Depends(get_db)):
    messages = db.query(Message).filter(Message.chat_id == chat_id).order_by(Message.created_at.desc()).limit(limit).all()
    return [
        {
            "id": m.id,
            "sender_id": m.sender_id,
            "text": m.text,
            "file_path": m.file_path,
            "file_name": m.file_name,
            "is_star_gift": m.is_star_gift,
            "star_cost": m.star_cost,
            "created_at": m.created_at.isoformat()
        }
        for m in reversed(messages)
    ]

# 8. ПОЛУЧИТЬ СПИСОК ЧАТОВ
@app.get("/chats/{user_id}")
async def get_user_chats(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    
    members = db.query(ChatMember).filter(ChatMember.user_id == user_id).all()
    chats = []
    for member in members:
        chat = db.query(Chat).filter(Chat.id == member.chat_id).first()
        if chat:
            # Получаем последнее сообщение
            last_msg = db.query(Message).filter(Message.chat_id == chat.id).order_by(Message.created_at.desc()).first()
            chats.append({
                "id": chat.id,
                "name": chat.name or "Чат",
                "type": chat.type,
                "last_message": last_msg.text if last_msg else None,
                "last_message_time": last_msg.created_at.isoformat() if last_msg else None
            })
    
    return chats

# 9. ПОЛУЧИТЬ ИНФОРМАЦИЮ О ПОЛЬЗОВАТЕЛЕ
@app.get("/user/{user_id}")
async def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    
    return {
        "id": user.id,
        "phone": user.phone,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_online": user.is_online,
        "last_seen": user.last_seen.isoformat(),
        "stars": user.stars_balance
    }

# 10. WEBSOCKET
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await manager.connect(user_id, websocket)
    db = SessionLocal()
    
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            
            if action == "typing":
                chat_id = data.get("chat_id")
                is_typing = data.get("is_typing", True)
                
                await manager.broadcast_to_chat(
                    chat_id,
                    {
                        "type": "typing",
                        "user_id": user_id,
                        "is_typing": is_typing
                    },
                    db,
                    exclude_user_id=user_id
                )
            
            elif action == "ping":
                await websocket.send_json({"type": "pong"})
            
            elif action == "new_message":
                # Сообщение уже сохранено через /send_message
                pass
    
    except WebSocketDisconnect:
        manager.disconnect(user_id, websocket)
    finally:
        db.close()

# 11. СТАТИЧЕСКИЕ ФАЙЛЫ (ВЕБ-ИНТЕРФЕЙС)
app.mount("/", StaticFiles(directory="static", html=True), name="static")

# 12. КОРНЕВОЙ ПУТЬ
@app.get("/api")
async def root():
    return {
        "name": "Арима Messenger",
        "version": "1.0.0",
        "status": "running",
        "endpoints": [
            "/register - Регистрация",
            "/login - Вход",
            "/balance/{user_id} - Баланс звёзд",
            "/buy_stars - Купить звёзды",
            "/transfer_stars - Перевести звёзды",
            "/send_message - Отправить сообщение",
            "/messages/{chat_id} - История чата",
            "/chats/{user_id} - Список чатов",
            "/user/{user_id} - Информация о пользователе",
            "/ws/{user_id} - WebSocket"
        ]
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
