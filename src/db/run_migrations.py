import asyncio
import os
from databases import Database
from pathlib import Path

async def run_migrations():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("❌ DATABASE_URL not set")
        return
    
    db = Database(database_url)
    await db.connect()
    print("✅ Connected to database")
    
    migration_file = Path(__file__).parent / "migrations" / "001_initial_schema.sql"
    
    print(f"📋 Running migration: {migration_file.name}")
    
    with open(migration_file, 'r') as f:
        sql = f.read()
    
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    
    for i, statement in enumerate(statements, 1):
        try:
            await db.execute(statement)
            print(f"  ✅ Statement {i}/{len(statements)}")
        except Exception as e:
            print(f"  ⚠️  Statement {i}: {str(e)}")
    
    await db.disconnect()
    print("✅ Migrations completed")

if __name__ == "__main__":
    asyncio.run(run_migrations())
