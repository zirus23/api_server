import sqlite3

"""
This file contains all functions that interact directly with the database
and use SQL commands to do so. This is done to keep the main file readable.
"""

#initializes the database with a users and a message table
def init_database(self):
    cursor = self.conn.cursor()
    
    # table of u_id, username and auth_token
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
    u_id INTEGER PRIMARY_KEY,
    username TEXT NOT NULL UNIQUE,
    auth_token TEXT NOT NULL
    );
    """)
    
    # table of messages with sender and receiver info
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
    msg_id INTEGER PRIMARY_KEY,
    sender INTEGER NOT NULL,
    recipient INTEGER NOT NULL,
    msg_type TEXT NOT NULL,
    msg_text TEXT,
    img_height INTEGER,
    img_width INTEGER,
    url TEXT,
    vid_source TEXT,
    timestamp TEXT NOT NULL
    );
    """)
    #NOTE: Ideally, we would update this table in batches instead
    # of with each message received, but for sake of time I'm not doing that