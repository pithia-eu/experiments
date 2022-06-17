# Rest API deployment with Docker

Instructions for manual deployment in docker engine

## Create the containers
1. Install docker engine
2. Create a mysql container, command: docker-compose up -d
3. Create the stim container, command: docker run --rm -d -p 8080:8080 --add-host=host.docker.internal:host-gateway --name my_stim dkagialis/stim-rest:0.0.1

## Configure mysql
1. Find the id of the container with docker ps
2. SSH in the container: docker exec -it 'mysql-id' /bin/bash
3. Initiate mysql command prompt: mysql -u root -ppassword
4. Execute: ALTER USER root IDENTIFIED WITH mysql_native_password BY "password";
5. exit the container

## Configure stim
1. Find the id of the container with docker ps
2. SSH in the container: docker exec -it 'stim-id' /bin/bash
3. Execute: sudo su
4. add mysql to hosts
   1. Execute: vi /etc/hosts
   2. Add 'mysql' right next tto the record host.docker.internal 
5. Exit sudo
6. Initiate the database: bash init.sh
7. Execute the model for the first time: bash ace.sh
8. Start the API
   1. Execute: cd API/
   2. Execute: uvicorn main:app --reload --host 0.0.0.0 --port 8080

## Use stim through the rest api
Go to the URL: http://host-ip/docs and see the swagger ui