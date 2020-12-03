FROM docker.elastic.co/elasticsearch/elasticsearch:7.10.0

ENV GRAFEAS_USER=grafeas
ENV GRAFEAS_PASSWORD=grafeas

EXPOSE 9200
EXPOSE 9300

RUN bin/elasticsearch-users useradd $GRAFEAS_USER -p $GRAFEAS_PASSWORD
