import os
from sqlalchemy import text
from connection import get_db_connection

def initialize_database():
    """
    Runs the schema.sql file to create tables in the Neon database.
    """
    engine = get_db_connection()
    
    # Define the path to your schema file
    schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
    
    print(f"🚀 Initializing tables in: {os.getenv('POSTGRES_DB')}...")
    
    try:
        with open(schema_path, 'r') as f:
            # Split commands by semicolon to execute them one by one
            sql_commands = f.read().split(';')
            
        with engine.connect() as connection:
            for command in sql_commands:
                if command.strip():
                    connection.execute(text(command))
                    connection.commit()
                    
        print("✅ Database initialization complete! Tables are ready.")
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")

if __name__ == "__main__":
    initialize_database()