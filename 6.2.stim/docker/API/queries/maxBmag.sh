#!/bin/bash
db=$(mysql -h mysql -u root -ppassword -e 'SHOW DATABASES;');
if (echo $db | grep 'server_dias_devel');
then

        source $CONDA_PREFIX/etc/profile.d/conda.sh
        conda activate ace
        cd /home/model/ACE/bin
        python queries.py maxbmag
fi
