#!/usr/bin/env python3

import json
import sqlite3
import datetime
from contextlib import closing

"""
This file contains all functions that interact directly with the database
and use SQL commands to do so.
"""

# the database will commit once every N users and once every N messages
# can modify this as per needs to prioritize operation speed or concurrency 
# with others reading the database
commit_frequency = 20

class Database():

    #initializes the database with a users and a message table
    def __init__(self, conn):
        self.conn = conn
        self.user_count = 0
        self.msg_count = 0
        cursor = self.conn.cursor()
        
        # Table of users
        # This table is double indexed which makes lookups by u_id/username
        # really fast but makes insertion of new elements slower. This is
        # desirable behavior since the percentage of queries for new users
        # being added is much lower than existing users being queried.
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
        u_id INTEGER PRIMARY_KEY,
        username TEXT NOT NULL UNIQUE,
        auth_token TEXT NOT NULL
        );
        """)

        # Table of messages
        # This table is not indexed since new elements will be inserted very
        # frequently so we don't want to slow down insertions. As a result,
        # this will have slower lookups but this can be countered by always
        # querying messages in batches (reasonable for most cases like 
        # getMessages when restoring client's message history to a new device)
        # This table should ideally also be updated in batches, so the server
        # can build a cache and update this table once every 60 seconds or so
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
        cursor.close()

    # checks if authorization header valid for given user id
    def authenticate(self, u_id, auth):
        if auth == None: return False
        
        auth_type, token = auth.split()
        if auth_type.lower() != "bearer": return False
        
        cursor = self.conn.cursor()
        auth_token = cursor.execute(
                  """SELECT auth_token FROM users WHERE u_id=?;""",
                  (int(u_id),)).fetchone()
        
        cursor.close()

        return (auth_token != None) and (auth_token[0] == token)

    # adds given username and token to users table, update usercount
    def add_user(self, user, token):
        cursor = self.conn.cursor()
        u_id = self.user_count

        request = "INSERT INTO users VALUES (?, ?, ?);"
        cursor.execute(request, (u_id, user, token))

        self.user_count += 1
        # don't need to update database in real time, commit occasionally
        if self.user_count % commit_frequency == 0:
            self.conn.commit()
        cursor.close()

        return u_id

    def get_messages(self, recipient, start, limit):
        cursor = self.conn.cursor()
        messages = cursor.execute("""
        SELECT * FROM messages WHERE recipient=? LIMIT ?, ?;
        """, (recipient, start, limit)).fetchall()
        cursor.close()

        parsed_messages = []
        # parsing messages into correct format
        for msg in messages:
            # msg_id, sender, recipient, msg_type, 
            # msg_text, img_height, img_width, url, vid_source, timestamp
            if msg[3] == "text":
                content = {
                           "type": msg[3], 
                           "text":msg[4]
                           }

            elif msg[3] == "image":
                content = {
                           "type": msg[3], 
                           "url":msg[7], 
                           "height":msg[5],
                           "width":msg[6]
                           }

            else:
                assert(msg[3] == "video")
                content = {
                           "type": msg[3], 
                           "url":msg[7], 
                           "source": msg[8]
                           }

            curr = {
                    "id": msg[0],
                    "timestamp": msg[9],
                    "sender": msg[1],
                    "recipient": msg[2],
                    "content": content
                   }

            parsed_messages += [curr]

        return json.dumps({"messages": parsed_messages})

    # parsed request body and adds message to the database
    def add_message(self, body):
        cursor = self.conn.cursor()

        sender = body["sender"]
        recipient = body["recipient"]
        content = body["content"]
        msg_type = content["type"]
        msg_id = self.msg_count

        #get timestamp (zero hour offset)
        now = datetime.datetime.utcnow()
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        # hardcoding metadata as requested
        if msg_type == "text":
            text = content["text"]
            request = """
            INSERT INTO messages(msg_id, sender, recipient, msg_type, 
                                 msg_text, timestamp)
            VALUES (?, ?, ?, ?, ?, ?);
            """
            cursor.execute(request, (msg_id, sender, recipient, msg_type, 
                                     text, timestamp))

        elif msg_type == "image":
            url = content["url"]
            height = content["height"]
            width = content["width"]
            request = """
            INSERT INTO messages(msg_id, sender, recipient, msg_type, 
                                 url, img_height, img_width, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
            cursor.execute(request, (msg_id, sender, recipient, msg_type, 
                                     url, height, width, timestamp))

        elif msg_type == "video":
            url = content["url"]
            source = content["source"]
            request = """
            INSERT INTO messages(msg_id, sender, recipient, msg_type, 
                                url, vid_source, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """
            cursor.execute(request, (msg_id, sender, recipient, msg_type,
                                     url, source, timestamp))
        else:
            # handled by caller
            raise Exception("Invalid message type\n")

        # update message count
        self.msg_count += 1
        # commit is slow but nescassary for others seeing the database to be
        # able to see this data, so we only commit occasionally
        if msg_count % commit_frequency == 0:
            self.conn.commit()
        
        cursor.close()

        # format response with message id and timestamp
        response = json.dumps({"id":msg_id, "timestamp":timestamp})
        return response

    # commits to database
    def commit(self):
        self.conn.commit()

    # check if database is healthy
    def query_health(self):
        with closing(self.server.conn.cursor()) as cur:
            cur.execute('SELECT 1')
            (res, ) = cur.fetchone()
            return res
