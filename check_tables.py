import asyncio
import os
from databases import Database

async def check_tables():
    database_url = os.getenv("DATABASE_URL")
    print(f"Connecting to database...")
    
    db = Database(database_url)
    await db.connect()
    print("Connected!")
    
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
    
    tables = await db.fetch_all(query)
    print(f"Found {len(tables)} tables:")
    
    for table in tables:
        print(f"  - {table[0]}")
    
    await db.disconnect()

asyncio.run(check_tables())
