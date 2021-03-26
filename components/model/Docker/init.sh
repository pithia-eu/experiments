#!/bin/bash
/etc/init.d/sshd start
source $CONDA_PREFIX/etc/profile.d/conda.sh
conda activate ace
cd /home/model/ACE/bin
python oper.py db -c
python oper.py db -i