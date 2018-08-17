#!/usr/bin/env bash

/data/anaconda3/bin/python ./script/aws_emr_launcher.py
if [ $? -ne 0 ]; then
    exit 1
fi

