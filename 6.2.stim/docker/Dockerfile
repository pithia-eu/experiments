FROM continuumio/miniconda3:4.9.2-alpine

# Create Home directory
RUN mkdir /home/model
WORKDIR /home/model

# Copy scripts
COPY init.sh /home/model/init.sh
COPY ace.sh /home/model/ace.sh

# Change permissions
RUN chmod 755 /home/model/init.sh
RUN chmod 755 /home/model/ace.sh

#Create the ACE corn job
RUN echo "*/5       *       *       *       *       /home/model/ace.sh" >> /etc/crontabs/root
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
ADD ACE /home/model/ACE/
RUN conda env create -f ./ACE/var/data/environment.yml
RUN apk add bash
RUN conda init bash
RUN apk add mysql-client
RUN apk add sudo

EXPOSE 22