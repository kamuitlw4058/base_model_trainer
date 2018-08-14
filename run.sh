#!/usr/bin/env bash

/data/anaconda3/bin/python ./script/aws_emr_launcher.py
if [ $? -ne 0 ]; then
    exit 1
fi

export CTR_USER_NAME=model
/data/anaconda3/bin/python ./bin/pctr.py -d no -s yes -m merge
