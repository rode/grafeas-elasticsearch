FROM elasticsearch:7.9.3

ENV GRAFEAS_USER=grafeas
ENV GRAFEAS_PASSWORD=grafeas

EXPOSE 9200
EXPOSE 9300

RUN bin/elasticsearch-users useradd $GRAFEAS_USER -p $GRAFEAS_PASSWORD