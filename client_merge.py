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

socket_lock = threading.Lock()

class ComputeNode:
    def __init__(self, server_addr, server_port, addr, port, peer_addrs, next_idx, id, health_port = 4000, timeout=5):
        self.server_addr = server_addr
        self.server_port = server_port
        self.addr = addr
        self.port = port

        self.peers = peer_addrs
        self.next_addr = peer_addrs[next_idx][0] 
        self.next_port = peer_addrs[next_idx][1] + 2

        self.timeout = 5
        self.in_peer_sockets = []
        self.out_peer_socket = None
        self.health_port = health_port
        self.id = id

        with open('model.pkl', 'rb') as file:
            self.model = torch.load(file)

    def process(self, x):    
        x = self.model.avgpool(x)
        x = self.model.classifier(y.view(y.size(0), -1))
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
                self.in_peer_sockets.append(socket)
                print(f"Connected to incoming socket from {addr}.")
                print(len(self.in_peer_sockets))
                if len(self.in_peer_sockets) == IN_PEERS:
                    break
            else:
                pass

    def out_peer_thread(self):
        timeout = 15 # seconds
        end_time = timeout + time.time()
        while time.time() < end_time:
            try:
                self.out_peer_socket.connect((self.next_addr, self.next_port))
                print("connected")
                break
            except:
                print("Failed to establish outgoing socket. Trying again.")
           
    def handle_in_thread_(self,idx):
        # print('started 1')
        print('Initiated thread to handle incoming messages.')
        global socket_lock
        while True:
            x = self.recvLargeMessage(self.in_peer_sockets[idx])
            print('received msg')
            if x is not None:
                x = x * 2 + torch.ones_like(x)
                with socket_lock:
                    self.sendLargeMessage(self.out_peer_socket, x)
                    print('handled message')
            else:
                pass


    def sendLargeMessage(self, socket, X, ):
        serialized_data = pickle.dumps(X)
        total_size = len(serialized_data)
        total_size = pack('>Q', len(serialized_data))

        socket.sendall(total_size)
        socket.sendall(serialized_data)
        print("Large message sent")

    def recvLargeMessage(self, socket, ):
        bs = socket.recv(8)
        (length,) = unpack('>Q', bs)
        data = b''
        while len(data) < length:
            to_read = length - len(data)
            data += socket.recv(4096 if to_read > 4096 else to_read)
        return pickle.loads(data)

    def sendHearbeats(self):
        host = self.server_addr
        port = self.health_port + self.id
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as health_socket:
            health_socket.connect((host, port))
            print(f"Connected to server at {host}:{port}")

        heartbeat_interval = 1  # seconds
        heartbeat_byte = b'\x00'  # Single byte to send as heartbeat
        try:
            while True:
                health_socket.sendall(heartbeat_byte)
                print("Heartbeat sent")
                time.sleep(heartbeat_interval)
        except KeyboardInterrupt:
            print("Client stopped")

    def run(self):
        global IN_PEERS
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
            print("Connecting to peer(s)...")
            threads = []

            in_thread = threading.Thread(target=self.in_peer_thread, )
            threads.append(in_thread)
            in_thread.start()


            #set next peer
            out_thread = threading.Thread(target=self.out_peer_thread)
            threads.append(out_thread)
            out_thread.start()

            for t in threads:
                t.join()
            print("Threads returned.")

            print(f"num peers {IN_PEERS}")
            for i in range(IN_PEERS):
                print('looking at peer', i)
                thread = threading.Thread(target=self.handle_in_thread_, args=([i]) )
                thread.start()

            while True:
                pass

if __name__ == '__main__':
    peers = [('127.0.0.1', 5000), ('127.0.0.2', 5000), ('127.0.0.3', 5000), ('127.0.0.4', 5000), ('127.0.0.5', 5000), ('127.0.0.6', 5000), ('127.0.0.7', 5000) ]

    idx = int(sys.argv[1])
    next_idx = int(sys.argv[2])
    myport = peers[idx][1]    
    myaddr = peers[idx][0]

    try:
        IN_PEERS = int(sys.argv[3])
    except:
        IN_PEERS = 1

    node = ComputeNode('localhost', 5000, myaddr, myport, peers, next_idx, idx)
    node.run()