import socket
import pickle
import select
import threading
import time
from struct import pack, unpack
import torch
from torchvision import datasets, transforms
from torch.utils.data import DataLoader
from torchvision import datasets
import os
import sys
import itertools

import numpy as np
BUSY_FLAGS = []

def sendArray(socket, array):
    serialized_data = pickle.dumps(array)
    total_size = pack('>Q', len(serialized_data))

    socket.sendall(total_size)
    socket.sendall(serialized_data)

def recvArray(socket):
    bs = socket.recv(8)
    (length,) = unpack('>Q', bs)
    data = b''
    while len(data) < length:
        to_read = length - len(data)
        data += socket.recv(4096 if to_read > 4096 else to_read)
    return pickle.loads(data)

def recvHeartbeat(socket, id):
    host = 'localhost'
    port = 4000 + id
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Server listening on {host}:{port}")

        conn, addr = server_socket.accept()
        with conn:
            print(f"Connected to {addr}")

            while True:
                heartbeat = conn.recv(1)
                if not heartbeat:
                    print("Connection closed")
                    break
                print("Heartbeat received")

def enterJob(socket, idx, x):
    global BUSY_FLAGS
    BUSY_FLAGS[idx] = True
    sendArray(socket, x)
    BUSY_FLAGS[idx] = False

    return 0

def main():
    host = 'localhost'
    port = 5000

    global N
    global P
    global batch_size
    global MAX_JOBS


    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Server listening on {host}:{port}")

        peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        peer_socket.bind((host, port+2))
        peer_socket.listen()

        clients = []
        timeout = 15 # seconds
        end_time = timeout + time.time()

        while time.time() < end_time:
            # Calculate remaining time until timeout
            remaining_time = end_time - time.time()
            if remaining_time <= 0:
                break

            # Use select to wait for connections with a timeout
            ready_sockets, _, _ = select.select([server_socket], [], [], remaining_time)

            if ready_sockets:
                client_socket, addr = server_socket.accept()
                print(f"Connected to client at {addr}")
                clients.append(client_socket)
            else:
                print(f"No more connections received within the timeout of {timeout} seconds")
                break

            if len(clients) == N:
                print(f"All {N} already connected!")
                break

        print('I got', len(clients), 'clients.')

        #check that everyone is still alive
        responses = []
        for idx, client_socket in enumerate(clients):
            message = 'are you alive?'
            client_socket.sendall(message.encode())
            resp = client_socket.recv(1024).decode()
            responses.append(resp)

        # if everyone is still alive, use the 'welcome' message to make them connect to their 
        # preselected 'next' peer 
        if len(responses) == len(clients):
            for idx, client_socket in enumerate(clients):
                message = 'welcome'
                client_socket.sendall(message.encode())

        # Use select to wait for connections with a timeout
        ready_sockets, _, _ = select.select([peer_socket], [], [], 15)

        if ready_sockets:
            peer_socket, addr = peer_socket.accept()
            print(f"Connected to client at {addr}")
        else:
            print(f"No peer connections received.")

        pipe_entrances = []
        threads = {}
        for i, c in  enumerate(clients[:P]):
            pipe_entrances.append({'socket':c, 'busy':False, 'idx': i})
            BUSY_FLAGS.append(False)
            # threads.append(None)

        # Initialize a round-robin
        pipe_entrances_iterator = itertools.cycle(pipe_entrances)
        waiting_for = 0

        data_path = './data/tiny-imagenet-200'

        # Define the transformations for the dataset
        transform = transforms.Compose([
            transforms.Resize((64, 64)),  # Resize images to 64x64
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])

        # Load the training and validation datasets
        # train_dataset = datasets.ImageFolder(os.path.join(data_path, 'train'), transform=transform)
        val_dataset = datasets.ImageFolder(os.path.join(data_path, 'val'), transform=transform)

        # Create DataLoaders for the training and validation datasets
        batch_size = 20
        # train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)

#############################################################
#############################################################
        timings = []

        cnt = 0
        for images, labels in val_loader:
            start_time = time.time()
            while True:
                pe = next(pipe_entrances_iterator)
                if not BUSY_FLAGS[pe['idx']]:
                    # if a thread was previously opened to send a job to a given client wait
                    try:
                        # instead of checking if key is in dict, just try to access it
                        # let python handle the error
                        threads[pe['idx']].join()
                    except:
                        pass
                    waiting_for += 1
                    t = threading.Thread( target = enterJob, args=( pe['socket'], pe['idx'], images) )
                    threads[pe['idx']] = t
                    t.start()
                    print(f'just sent msg to {pe['idx']}')
                    break
                else:
                    pass


            for pe in pipe_entrances:
                i = pe['idx']
                try:
                    threads[pe['idx']].join()
                except:
                    pass
            if waiting_for >= 20:

                for i in range(waiting_for):
                    y = recvArray(peer_socket)
                    print('received back:',type(y))
                    cnt += 1
                    timings.append(time.time() - start_time)


                waiting_for = 0
            if cnt >= MAX_JOBS:
                break

        print("All jobs done.")
        print("avg time:", np.mean(timings))
        while True:
            pass


#############################################################
#############################################################

if __name__ == '__main__':
    N = int(sys.argv[1]) #num of total worker nodes
    P = int(sys.argv[2]) #num of parallel workers
    batch_size = int(sys.argv[3]) #batch_size  
    MAX_JOBS = int(sys.argv[4])     #maximum number of jobs to be submitted
    main()