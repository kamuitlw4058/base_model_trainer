#!/data/anaconda3/bin/python

import json
import os
import sys


def getenv(key,default="",verbose=False):
    envVal=os.getenv('ZAMPDA_HOME')
    if envVal == None :
        envVal = default

    if  verbose:
        print("ENV " + key +  ":" + str(envVal))

    return envVal

def run_cmd(cmd):
    try:
        import subprocess
    except ImportError:
        _, result_f, error_f = os.popen3(cmd)
    else:
        process = subprocess.Popen(cmd, shell=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result_f, error_f = process.stdout, process.stderr

    errors = error_f.read()
    if errors:  print(errors)
    result_str = result_f.read().strip()
    if result_f:   result_f.close()
    if error_f:    error_f.close()

    return result_str.decode()
