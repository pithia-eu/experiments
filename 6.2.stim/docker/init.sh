#!/bin/bash
/etc/init.d/sshd start
echo '%wheel ALL=(ALL) ALL' > /etc/sudoers.d/wheel
adduser model wheel
while true
do
db_srv=$(mysqladmin status -h mysql -u root -ppassword);
if echo $db_srv | grep 'Uptime';
then
        echo "MySQL is up. Trying to create the database." >> /home/model/init.log;
        date >> /home/model/init.log;
        source $CONDA_PREFIX/etc/profile.d/conda.sh
        conda activate ace
        pip install pandas
        cd /home/model/ACE/bin
        python oper.py db -c >> /home/model/init.log;
        python oper.py db -i >> /home/model/init.log;
        echo "Completed, check DB status." >> /home/model/init.log;
        date >> /home/model/init.log;
        echo "-----------------------------------" >> /home/model/init.log;
        break;
else
        echo "MySQL is down." >> /home/model/init.log;
        date >> /home/model/init.log
        echo "-----------------------------------" >> /home/model/init.log;
fi
done
