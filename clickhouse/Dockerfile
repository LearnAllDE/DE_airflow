FROM yandex/clickhouse-server

MAINTAINER Alexander Somik

RUN apt-get -yqq update

RUN apt-get -yqq install python3 python3-pip

ADD client_place /opt/client_place

WORKDIR /opt/client_place

RUN pip3 install -r requirements.txt

EXPOSE 5000

CMD ["python3", "./app.py"]
