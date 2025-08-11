# Use a imagem oficial do Logstash como base
FROM docker.elastic.co/logstash/logstash:8.10.0

# Defina o diretório de trabalho
WORKDIR /usr/share/logstash

# Instale o plugin do Logstash para o InfluxDB
RUN bin/logstash-plugin install logstash-output-influxdb

# Copie o arquivo de configuração do Logstash para o contêiner
COPY pipeline/logstash.conf /usr/share/logstash/pipeline/logstash.conf
