"""
Setup script to create MySQL database and tables for POSP Payout Checker
Run this FIRST before importing Excel data

Usage: python setup_database.py
"""

import mysql.connector
from mysql.connector import Error
from config import (
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, TABLES
)

def create_database():
    """Create the main database"""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        print(f"[OK] Database '{DB_NAME}' created/verified")
        
        cursor.close()
        conn.close()
        return True
    except Error as e:
        print(f"[ERROR] Error creating database: {e}")
        return False

def create_tables():
    """Create all required tables"""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()
        
        for table_name, table_sql in TABLES.items():
            try:
                cursor.execute(table_sql)
                print(f"[OK] Table '{table_name}' created/verified")
            except Error as e:
                print(f"[ERROR] Error creating table '{table_name}': {e}")
        
        cursor.close()
        conn.close()
        return True
    except Error as e:
        print(f"[ERROR] Error connecting to database: {e}")
        return False

def verify_setup():
    """Verify all tables exist"""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s
        """, (DB_NAME,))
        
        tables = cursor.fetchall()
        print(f"\n[VERIFY] Database Schema Verification:")
        print(f"   Database: {DB_NAME}")
        print(f"   Tables created: {len(tables)}")
        for table in tables:
            print(f"   [OK] {table[0]}")
        
        cursor.close()
        conn.close()
        return len(tables) == len(TABLES)
    except Error as e:
        print(f"[ERROR] Error verifying setup: {e}")
        return False

def main():
    """Main setup routine"""
    print("=" * 60)
    print(" POSP PAYOUT CHECKER - DATABASE SETUP")
    print("=" * 60)
    print(f"\n[CONFIG]:")
    print(f"   Host: {DB_HOST}:{DB_PORT}")
    print(f"   User: {DB_USER}")
    print(f"   Database: {DB_NAME}")
    print("\n[SETUP] Creating database structure...\n")
    
    # Step 1: Create database
    if not create_database():
        print("\n[ERROR] Setup failed at database creation")
        return False
    
    # Step 2: Create tables
    if not create_tables():
        print("\n[ERROR] Setup failed at table creation")
        return False
    
    # Step 3: Verify
    print("\n" + "=" * 60)
    if verify_setup():
        print("\n[SUCCESS] DATABASE SETUP COMPLETED SUCCESSFULLY!")
        print("\n[NEXT STEPS]:")
        print("   1. Create .env file with your MySQL password")
        print("   2. Run: python import_excel.py")
        print("   3. Run: python -m uvicorn app:app --reload")
    else:
        print("\n[WARN] Setup completed but verification failed")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
