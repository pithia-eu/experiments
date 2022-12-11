#!/bin/bash

cd /home/ubuntu/experiments/dtm
uvicorn main:app --reload --port 8080 --host 0.0.0.0