#!/usr/bin/env python3

import os
import json
import sqlite3
import hashlib
import datetime
import contextlib
import http.server

conn = sqlite3.connect("challenge.db")

class Handler(http.server.BaseHTTPRequestHandler):
    def json_app_respond(self, code=400, message=""):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(message.encode("UTF-8"))
    
    def check_authorization(self):
        self.auth_user = None
        has_auth = self.headers.get("Authorization")
        if has_auth != None:
            auth_type, token = has_auth.split()
            if auth_type.lower() != "bearer": return

            cursor = conn.cursor()
            self.auth_user = cursor.execute("""SELECT u_id 
                        FROM users WHERE auth_token=?;""", (token,)).fetchone()
            if self.auth_user != None: 
                self.auth_user = self.auth_user[0]

        print("current user : ", self.auth_user)

    def do_POST(self):
        self.check_authorization()

        if self.path == "/check":
            self.handle_check()
        elif self.path == "/users":
            self.handle_new_user()
        elif self.path == "/login":
            self.handle_login()
        elif self.path == "/messages":
            self.handle_send_message()
        else:
            self.json_app_respond(404, "URL not found\n")

        print("posted headers : ", self.headers)
        
    def do_GET(self):
        self.check_authorization()
        
        if (self.path[:9] == "/messages"):
            try:
                self.handle_messages()
            except:
                pass
        else:
            self.json_app_respond(404, "Invalid URL: %s" % self.path)

    #NOTE: ASSUMING QUERY PARAMS PASSED LIKE /messages?recipient=0&start=1
    def handle_messages(self):
        #get sender id, recipient id and type of message
        params = self.path.split("?")[-1].split("&")
        params = dict([tuple(p.split("=")) for p in params])
        recipient = params["recipient"]
        start = params["start"]
        if "limit" in params:
            limit = params["limit"]
        else:
            limit = "100" #default

        #only valid if the authorization token was obtained by recipient login
        if int(recipient) != self.auth_user:
            self.json_app_respond(400, 
            "Authentication failed, incorrect token for recipient\n")
            return
        
        cursor = conn.cursor()
        messages = cursor.execute("""
        SELECT * FROM messages WHERE recipient=? LIMIT ?, ?;
        """, (recipient, start, limit)).fetchall()

        parsed = []
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
            parsed += [curr]

        self.json_app_respond(200, json.dumps({"messages": parsed}))

    def handle_send_message(self):
        #get sender id, recipient id and type of message
        content_len = int(self.headers.get("content-length"))
        body = json.loads(self.rfile.read(content_len))
        sender = body["sender"]
        recipient = body["recipient"]
        msg_type = body["content"]["type"]

        #only allowed if sender is
        if sender != self.auth_user:
            self.json_app_respond(400, 
            "Authentication failed, incorrect token for sender\n")
            return

        cursor = conn.cursor()
        msg_id = cursor.execute("SELECT COUNT(*) FROM messages;").fetchone()[0]

        #get timestamp
        now = datetime.datetime.utcnow()
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        #NOTE: hardcoding here since writeup wanted, alternative is to just add
        # a single content text field in data base and store json.dumps(content)
        if msg_type == "text":
            text = body["content"]["text"]

            request = """
            INSERT INTO messages(msg_id, sender, recipient, msg_type, 
                                 msg_text, timestamp)
            VALUES (?, ?, ?, ?, ?, ?);
            """
            cursor.execute(request, (msg_id, sender, recipient, msg_type, 
                                     text, timestamp))

        elif msg_type == "image":
            url = body["content"]["url"]
            height = body["content"]["height"]
            width = body["content"]["width"]
            request = """
            INSERT INTO messages(msg_id, sender, recipient, msg_type, 
                                 url, img_height, img_width, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
            cursor.execute(request, (msg_id, sender, recipient, msg_type, 
                                     url, height, width, timestamp))

        elif msg_type == "video":
            url = body["content"]["url"]
            source = body["content"]["source"]
            request = """
            INSERT INTO messages(msg_id, sender, recipient, msg_type, 
                                url, vid_source, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """
            cursor.execute(request, (msg_id, sender, recipient, msg_type,
                                     url, source, timestamp))
        else:
            self.json_app_respond(400, "Invalid message type\n")
            return

        #commit once every N messages
        if msg_id % 20 == 0:
            conn.commit()
        #NOTE: Ideally we would store messages in a cache and insert in batches

        #respond with message id and timestamp (zero hour offset)
        message = json.dumps({"id":msg_id, "timestamp":timestamp})
        self.json_app_respond(200, message)

    def handle_login(self):
        #get username and password
        user, password = self.get_user()

        #generate token
        token = hashlib.md5((password + "salt").encode("UTF-8"))
        token = token.hexdigest()

        #check database for user
        cursor = conn.cursor()
        check = cursor.execute("""
                SELECT u_id, auth_token FROM users where username=? 
                AND auth_token=?;""", (user, token)).fetchone()

        # if valid user/pass combo, respond with u_id and token
        if check == None:
            self.json_app_respond(400, "Invalid username/password")
        else:
            u_id, auth_token = check
            message = json.dumps({"id":u_id, "token":auth_token})
            self.json_app_respond(200, message)

    def handle_new_user(self):
        #get username and password
        user, password = self.get_user()

        #generate auth token (normally would use a random salt like os.urandom)
        token = hashlib.md5((password + "salt").encode("UTF-8"))
        token = token.hexdigest()

        #add user and auth token to database
        cursor = conn.cursor()
        u_id = cursor.execute("SELECT COUNT(*) FROM users;").fetchone()[0]
        request = "INSERT INTO users VALUES (?, ?, ?);"
        try:
            cursor.execute(request, (u_id, user, token))
        except:
            self.json_app_respond(400, "User already exists\n")
            return

        #commit new user to database immediately
        conn.commit()

        #response of form {"id":id_num}
        message = json.dumps({"id": u_id})
        self.json_app_respond(200, message)

    def get_user(self):
        #get message body
        content_len = int(self.headers.get("content-length"))
        body = json.loads(self.rfile.read(content_len))
        return body["username"], body["password"]

    def handle_check(self):
        if self.query_health() != 1:
            raise Exception('unexpected query result')
        self.json_app_respond(200, json.dumps({"health": "ok"}))

    def query_health(self):
        with contextlib.closing(self.server.conn.cursor()) as cur:
            cur.execute('SELECT 1')
            (res, ) = cur.fetchone()
            return res

class Server(http.server.HTTPServer):
    def __init__(self, address, conn):
        super().__init__(address, Handler)
        self.conn = conn
        self.specs = json.loads(open("swagger.json","r").read())
        self.init_database()
    
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
        # of with message received

def main():
    conn = sqlite3.connect('challenge.db')
    address = ('localhost', 8080)
    httpd = Server(address, conn)
    httpd.serve_forever()
    conn.close() #kinda useless here but i dont like not having this

if __name__ == '__main__':
    main()
