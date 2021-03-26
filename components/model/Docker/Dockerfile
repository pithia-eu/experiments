FROM continuumio/miniconda3:4.9.2-alpine

RUN mkdir /home/model
WORKDIR /home/model

COPY ./cronjob.sh /home/model/cronjob.sh
COPY ./test.py /home/model/test.py
RUN chmod 755 /home/model/cronjob.sh
RUN echo "*       *       *       *       *       /home/model/cronjob.sh" >> /etc/crontabs/root
CMD ["crond", "-f", "-l", "8"]

RUN apk add openssh
RUN apk add openrc --no-cache

RUN rc-update add sshd
RUN rc-status

#RUN mkdir -p /run/openrc
RUN touch /run/openrc/softlevel
#RUN /etc/init.d/sshd restart
RUN adduser -D -h /home/model -s /bin/sh -g root -G root -u 1000 model
RUN echo 'model:model' | chpasswd

RUN apk add mysql-client
ADD ./ACE/ /home/model/ACE/
RUN conda env create -f ./ACE/var/data/environment.yml

EXPOSE 22