#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os


def print_dir(cwd=os.getcwd()):
    print(f'ls: {cwd}/')
    for f in os.listdir(cwd):
        if os.path.isdir(os.path.join(cwd, f)):
            print(f'-- {f}/')
        else:
            print(f'-- {f}')