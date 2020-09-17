FROM puckel/docker-airflow:latest

CMD ["webserver"]

MAINTAINER Alexander Somik

RUN apt-get -yqq update

RUN apt-get -yqq install python3 python3-pip

ADD airflow/dags /usr/local/airflow/dags

ADD airflow/requirements.txt /opt/lib/requirements.txt

WORKDIR /opt/lib/

RUN pip3 install -r requirements.txt
 

