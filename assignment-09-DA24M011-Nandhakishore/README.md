[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/tKoQrp-x)

Submitted by: **Nandhakishore C S** <br>
Roll Number: **DA24M011**

## Setup
1. Unzip the data file using CLI: <br>
(The data file is not added in this github reporitory, Only the .paraquet file is used for making the script)
```console
$ tar -xvzf data.tgz 
```

2. Create a Python virtual environment and install the dependecies from requirements.txt <br>
The Hugging face's transforms library requires either Tensorflow or PyTorch to be installed in the local system. In Mac OS, tensorflow is supported for 
```console
$ python3.9 -m venv .venv
$ source .venv/bin/activate 
(.venv) $ pip install -r requirements.txt
```

## Task1

To setup virtual spark clusters and using sentiment analysis from transformers library, run the first file. I have taken only first 5000 data points for the task for ease of computation.  
```console
(.venv) $ python3 task1.py 
```

## Task2

To get Recal and Precision for the model using map reduce paradigm run the file named task2.py; The confusion matrix is saved locally. 
```console
(.venv) $ python3 task2.py 
```