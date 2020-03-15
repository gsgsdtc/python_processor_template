import os
from os import path

def readText(path):
    with open(path, 'rb') as f:
        data = f.read()
        text = data.decode('utf-8')
        return text;


def writeText(path,text):
    with open(path, 'wt') as f:
        f.write(text)
        return;