FROM puckel/docker-airflow:latest

MAINTAINER Alexander Somik


#RUN apt-get -yqq update

#RUN apt-get -yqq install python3 python3-pip

ADD dags /usr/local/airflow/dags

ADD requirements.txt /opt/lib/requirements.txt

#ADD home/aquafeet/dep/data /opt/data

WORKDIR /opt/lib



RUN pip install -r requirements.txt

#CMD ["webserver"]

