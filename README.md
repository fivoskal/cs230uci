# cs230uci
distributed implementation of a neural net

The server is run using positional arguments defining the number of total compute nodes, num parallelly working nodes, batch_size, num of total jobs
e.g., ```python server.py 5 4 16 100```

Initialize as many parallel workers needed, <br>
```python client.py 1 5 ```, <br>
```python client.py 2 5 ```, <br>
```python client.py 3 5 ```, <br>
```python client.py 4 5 ```.
First argument is the id of the compute node, second is the next node along the pipeline.

Initialize the merger node,<br>
```python client_merge.py 5 0 4```.

First argument is the id of the compute node, second is the next node along the pipeline, third is the number of incoming parallel workers. 
