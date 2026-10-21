import aiosqlite

DB_NAME = "krishi_saathi.db"

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS farmers (
                phone_number TEXT PRIMARY KEY,
                onboarding_complete INTEGER DEFAULT 0,
                onboarding_step TEXT DEFAULT 'AWAITING_LANG',
                language TEXT DEFAULT 'mr',
                variety TEXT DEFAULT 'Thompson Seedless',
                pruning_date TEXT,
                district TEXT DEFAULT 'Nashik',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT,
                role TEXT,
                content TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (phone_number) REFERENCES farmers (phone_number)
            )
        """)
        await db.commit()

async def get_farmer(phone: str) -> dict | None:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM farmers WHERE phone_number = ?", (phone,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

async def save_or_update_farmer(phone: str, **kwargs):
    farmer = await get_farmer(phone)
    async with aiosqlite.connect(DB_NAME) as db:
        if not farmer:
            columns = ["phone_number"] + list(kwargs.keys())
            placeholders = ["?"] * len(columns)
            values = [phone] + list(kwargs.values())
            query = f"INSERT INTO farmers ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            await db.execute(query, values)
        else:
            set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
            values = list(kwargs.values()) + [phone]
            query = f"UPDATE farmers SET {set_clause} WHERE phone_number = ?"
            await db.execute(query, values)
        await db.commit()

async def reset_farmer(phone: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM chat_history WHERE phone_number = ?", (phone,))
        await db.execute("DELETE FROM farmers WHERE phone_number = ?", (phone,))
        await db.commit()

async def save_chat_turn(phone: str, role: str, content: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO chat_history (phone_number, role, content) VALUES (?, ?, ?)",
            (phone, role, content)
        )
        await db.commit()

async def save_chat_exchange(phone: str, user_content: str, model_content: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO chat_history (phone_number, role, content) VALUES (?, 'user', ?), (?, 'model', ?)",
            (phone, user_content, phone, model_content)
        )
        await db.commit()

async def get_recent_history(phone: str, limit: int = 4) -> list[dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT role, content FROM chat_history WHERE phone_number = ? ORDER BY id DESC LIMIT ?",
            (phone, limit)
        ) as cursor:
            rows = await cursor.fetchall()
            return [{"role": row["role"], "content": row["content"]} for row in reversed(rows)]