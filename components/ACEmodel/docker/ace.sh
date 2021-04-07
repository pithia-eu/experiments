#!/bin/bash
db=$(mysql -h mysql -u root -ppassword -e 'SHOW DATABASES;');
if (echo $db | grep 'server_dias_devel') && !(ps aux | pgrep python);
then
        echo "Starting ACE model." >> /home/model/ace.log;
        date >> /home/model/ace.log;
        echo "-----------------------------------" >> /home/model/ace.log;
        source $CONDA_PREFIX/etc/profile.d/conda.sh
        conda activate ace
        cd /home/model/ACE/bin
        python acebin.py
        echo "Check for results" >> /home/model/ace.log;
        date >> /home/model/ace.log;
        echo "-----------------------------------" >> /home/model/ace.log;
        break;
else
        echo "Cannot run the model." >> /home/model/ace.log;
        date >> /home/model/ace.log;
        echo "-----------------------------------" >> /home/model/ace.log;
fi