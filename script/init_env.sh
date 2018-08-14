#!/usr/bin/env bash
set -e

echo '----- emr bootstrap -----'


MY_HOME='/home/ubuntu'
cd $MY_HOME

aws s3 cp s3://pctr/python3.tar.gz .
tar zxf python3.tar.gz
rm python3.tar.gz

export PATH=$MY_HOME/python3/bin:$PATH


python_path=`which python`

echo "python path = $python_path"
echo '----- finish bootstrap -----'

