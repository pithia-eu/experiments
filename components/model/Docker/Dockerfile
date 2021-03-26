FROM continuumio/miniconda3:4.9.2-alpine

#Create Home directory
RUN mkdir /home/model
WORKDIR /home/model

#Create the ACE corn job
COPY ./cronjob.sh /home/model/cronjob.sh
COPY ./test.py /home/model/test.py
RUN chmod 755 /home/model/cronjob.sh
RUN echo "*       *       *       *       *       /home/model/cronjob.sh" >> /etc/crontabs/root
CMD ["crond", "-f", "-l", "8"]

#Enable SSH
RUN apk add openssh
RUN apk add openrc --no-cache
RUN rc-update add sshd
RUN rc-status
RUN touch /run/openrc/softlevel

#Add user
RUN adduser -D -h /home/model -s /bin/sh -g root -G root -u 1000 model
RUN echo 'model:model' | chpasswd

#Configure model env
COPY init.sh /home/model/init_db.sh
ADD ./ACE/ /home/model/ACE/
RUN conda env create -f ./ACE/var/data/environment.yml
RUN apk add bash
RUN conda init bash

EXPOSE 22