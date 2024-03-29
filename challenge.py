#!/usr/bin/env python3

"""
This file implements a basic http server as defined by the openAPI
specification swagger.json. It can be run with a simple
$ python3 challenge.py
This file needs to be in the same directory as db_utils.py to run
properly. A local database "challenge.db" will be created if doesn't
exist. If crashed, the database will still have information up until
the last commit. The commit frequency can be defined in the db_utils.py
file.
"""

import os
import json
import sqlite3
import hashlib
import datetime
import contextlib
import http.server
from db_utils import Database

# class to handle all requests
class Handler(http.server.BaseHTTPRequestHandler):

    # handle post requests
    def do_POST(self):
        if self.path == "/check":
            self.handle_check()
        elif self.path == "/users":
            self.handle_createUser()
        elif self.path == "/login":
            self.handle_login()
        elif self.path == "/messages":
            self.handle_sendMessage()
        else:
            self.simple_respond(404, f"URL not found: {self.path}")

    # handle get requests
    def do_GET(self):
        if (self.path[:9] == "/messages"):
            try:
                self.handle_getMessages()
            except:
                self.simple_respond(404, f"URL not found: {self.path}")
        else:
            self.simple_respond(404, f"URL not found: {self.path}")

    # handle request to check server health
    def handle_check(self):
        if self.server.db.query_health() == 1:
            self.simple_respond(200, json.dumps({"health": "ok"}))
            return
        # else the database is corrupted
        self.simple_respond(400, json.dumps({"health": "bad: data corrupted"}))
    
    # handles query to send message from sender to recipient
    def handle_sendMessage(self):
        #get sender id, recipient id and type of message
        try:
            body = self.get_body()
            sender = body["sender"]
        except:
            self.simple_respond(400, "Invalid body json formatting\n")
            return
        
        # check if authenticated to send message from sender
        auth = self.headers.get("Authorization")
        if self.server.db.authenticate(sender, auth):
            try:
                response = self.server.db.add_message(body)
                self.simple_respond(200, response)

            except Exception as type_error:
                self.simple_respond(400, str(type_error))

        else:
            self.simple_respond(400, 
            "Authentication failed, incorrect token for sender\n")

    def handle_login(self):
        #get username and password
        try:
            user, password = self.parse_user()
        except:
            self.simple_respond(400, "Invalid json formatting\n")
            return

        # generate token
        pass_bytes = (password + "salt").encode("UTF-8")
        token = hashlib.md5(pass_bytes).hexdigest()

        # check if user, token combo in database
        check = self.server.db.user_lookup(user, token)

        # if valid user/pass combo, respond with u_id and token
        if check == None:
            self.simple_respond(400, "Invalid username/password")
        else:
            u_id, auth_token = check
            message = json.dumps({"id":u_id, "token":auth_token})
            self.simple_respond(200, message)

    def handle_createUser(self):
        # get username and password
        try:
            user, password = self.parse_user()
        except:
            self.simple_respond(400, "Invalid json formatting\n")
            return

        # generate auth token
        # using a slower hash like bcrypt is better and harder to decode
        # the salt should fixed for a given database but chosen randomly
        token = hashlib.md5((password + "salt").encode("UTF-8"))
        token = token.hexdigest()

        # add user and auth token to database
        try:
            u_id = self.server.db.add_user(user, token)
        except:
            self.simple_respond(400, "User already exists\n")
            return

        # commit to database with each user for ease of testing
        # on a real server we would instead add the user to a cache and update
        # the database in batches
        self.server.db.commit()

        #response of form {"id":id_num}
        response = json.dumps({"id": u_id})
        self.simple_respond(200, response)

    # handles query to get limit number of messages with ids starting at start
    def handle_getMessages(self):
        # extract params recipient, start and optionally limit
        try:
            params = self.extract_query_params()
            start, recipient, limit = params
        except:
            self.simple_respond(400, "Invalid query formatting\n")
            return
        
        # check if authorized to get recipient's messages
        auth = self.headers.get("Authorization")
        if self.server.db.authenticate(recipient, auth):
            messages = self.server.db.get_messages(recipient, start, limit)
            self.simple_respond(200, messages)
        else:
            self.simple_respond(400, "Authentication failed\n")


    #########################################################
    ### ALL FUNCTIONS AFTER HERE ARE HELPERS FOR HANDLERS ###
    #########################################################

    # extracts start, recipient and limit params from query url
    def extract_query_params(self):
        params = self.path.split("?")[-1].split("&")
        params = dict([tuple(p.split("=")) for p in params])
        recipient = params["recipient"]
        start = params["start"]
        limit = params["limit"] if "limit" in params else "100"
        return start, recipient, limit

    # parses post body to extract username and password
    def parse_user(self):
        body = self.get_body()
        return body["username"], body["password"]

    # helper to easily respond with frequently used headers
    def simple_respond(self, code=200, response=""):
        # indicates success (200) or type of failure (eg. 404)
        self.send_response(code)

        #if success, send json application else send plaintext error
        if (code == 200):
            self.send_header('Content-Type', 'application/json')
        else:
            self.send_header('Content-Type', 'text/plain')
        self.end_headers()

        # send the body
        self.wfile.write(response.encode("UTF-8"))

    # extract body of a post request
    def get_body(self):
        content_len = int(self.headers.get("content-length"))
        body = json.loads(self.rfile.read(content_len))
        return body

# server class
class Server(http.server.HTTPServer):
    def __init__(self, address, conn):
        super().__init__(address, Handler)
        self.conn = conn
        self.db = Database(conn)

# main function
def main():
    # instantiate database
    conn = sqlite3.connect('challenge.db')

    # intialize the server class and serve forever
    address = ('localhost', 8080)
    httpd = Server(address, conn)
    httpd.serve_forever()

if __name__ == '__main__':
    main()
