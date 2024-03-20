import socket
import pickle
import random
import time
import sys
import os

import threading
import select
from struct import pack, unpack
# from struct import pack
import numpy as np
import torch
torch.set_num_threads(2)

class ComputeNode:
    def __init__(self, server_addr, server_port, addr, port, peer_addrs, next_idx, id, first=True, health_port = 4000, timeout=5):
        self.server_addr = server_addr
        self.server_port = server_port
        self.addr = addr
        self.port = port

        self.peers = peer_addrs
        self.next_addr = peer_addrs[next_idx][0] 
        self.next_port = peer_addrs[next_idx][1] + 2

        self.timeout = 5
        self.in_peer_socket = None
        self.out_peer_socket = None
        self.health_port = health_port # for future hearbeat implementation
        self.id = id

        self.FIRST = first
        with open('model.pkl', 'rb') as file:
            self.model = torch.load(file)

    def process(self, x):   
        x = self.model.features(x)
        return x
    
    def in_peer_thread(self):
        timeout = 15 # seconds
        end_time = timeout + time.time()
        while time.time() < end_time:
            # Calculate remaining time until timeout
            remaining_time = end_time - time.time()
            if remaining_time <= 0:
                break
            print("Waiting to listen for an incoming connection.")
            ready_socket, _, _ = select.select([self.in_peer_socket], [], [], self.timeout)
            if ready_socket:
                socket, addr = self.in_peer_socket.accept()
                self.in_peer_socket = socket
                print(f"Connected to incoming socket from {addr}.")
                break
            else:
                pass

    def out_peer_thread(self):
        timeout = 15 # seconds
        end_time = timeout + time.time()
        while time.time() < end_time:
            try:
                self.out_peer_socket.connect((self.next_addr, self.next_port))
                print(f"outgoing connection to {self.next_addr, self.next_port} established.")
                break
            except:
                print("Failed to establish outgoing socket. Trying again.")
    
    def sendLargeMessage(self, socket, X, ):
        serialized_data = pickle.dumps(X)
        total_size = len(serialized_data)
        total_size = pack('>Q', len(serialized_data))

        socket.sendall(total_size)
        socket.sendall(serialized_data)
        print("Large message sent")

    def recvLargeMessage(self, socket, ):
        # Receive the total size of the serialized data first
        bs = socket.recv(8)
        (length,) = unpack('>Q', bs)
        data = b''
        while len(data) < length:
            to_read = length - len(data)
            data += socket.recv(4096 if to_read > 4096 else to_read)
        return pickle.loads(data)
    
    # def sendHearbeats(self):
    #     host = self.server_addr
    #     port = self.health_port + self.id
    #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as health_socket:
    #         health_socket.connect((host, port))
    #         print(f"Connected to server at {host}:{port}")

    #     heartbeat_interval = 1  # seconds
    #     heartbeat_byte = b'\x00'  # Single byte to send as heartbeat
    #     try:
    #         while True:
    #             health_socket.sendall(heartbeat_byte)
    #             print("Heartbeat sent")
    #             time.sleep(heartbeat_interval)
    #     except KeyboardInterrupt:
    #         print("Client stopped")

    def run(self):
        # Initialize socket meant to connect to server
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        print(self.addr, self.port)
        server_socket.bind((self.addr, self.port))
        server_socket.connect((self.server_addr, self.server_port))

        # set up a socket for incoming peer connections
        in_peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        in_peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        in_peer_socket.bind((self.addr, self.port+2))
        in_peer_socket.listen()
        self.in_peer_socket = in_peer_socket

        # set up a socket for outgoing peer connections
        out_peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        out_peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        out_peer_socket.bind((self.addr, self.port+3))
        self.out_peer_socket = out_peer_socket

        message = server_socket.recv(1024).decode()
        if message == 'are you alive?':
            server_socket.sendall('yes'.encode())
            
        message = server_socket.recv(1024).decode()
        if message == 'welcome':
            print("Connecting to peer...")


            threads = []
            # set previous peer
            if not self.FIRST:
                in_thread = threading.Thread(target=self.in_peer_thread, )
                threads.append(in_thread)
                in_thread.start()
            else:
                self.in_peer_socket = server_socket

            #set next peer
            out_thread = threading.Thread(target=self.out_peer_thread)
            threads.append(out_thread)
            out_thread.start()

            for t in threads:
                t.join()
            print("Threads returned.")

            while True:
                x = self.recvLargeMessage(self.in_peer_socket)
                x = self.process(x)

                self.sendLargeMessage(self.out_peer_socket, x)

if __name__ == '__main__':
    peers = [('127.0.0.1', 5000), ('127.0.0.2', 5000), ('127.0.0.3', 5000), ('127.0.0.4', 5000), ('127.0.0.5', 5000), ('127.0.0.6', 5000), ('127.0.0.7', 5000) ]

    idx = int(sys.argv[1])          # my id
    next_idx = int(sys.argv[2])     # the next node
    myport = peers[idx][1]          # my address
    myaddr = peers[idx][0]          # my main port

    node = ComputeNode('localhost', 5000, myaddr, myport, peers, next_idx, idx)
    node.run()