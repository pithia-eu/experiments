#!/bin/bash
apt update
apt install vim git mysql-client ssh 
wget -P /tmp https://repo.anaconda.com/archive/Anaconda3-2020.11-Linux-x86_64.sh
bash /tmp/Anaconda3-2020.11-Linux-x86_64.sh
source ~/.bashrc
conda info