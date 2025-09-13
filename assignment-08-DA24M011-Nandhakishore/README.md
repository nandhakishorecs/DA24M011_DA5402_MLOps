[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/uvdj323_)


**Submitted by**: Nandhakishore C S<br>
**Roll Number**: DA24M011

## Setup: 
Create a Python virtual environment and install the dependecies from requirements.txt 

```console
$ python3.9 -m venv .venv
$ source .venv/bin/activate 
```

## For task 1: 
Run the Python file: 
```console
$ (.venv) python3 module1.py
```
Start the MlFlow Server (using port number 8000): 
```console
mlflow server --host 127.0.0.1 --port 8000
```

## For task 2
Createa a .json dump of an image
```console
(.venv) $ python3 preprocess_module2.py
```
Getting predictions using curl: 
```console
(.venv) $ curl -X POST -H "Content-Type: application/json" -d @image_data.json http://127.0.0.1:54320/invocations
```


