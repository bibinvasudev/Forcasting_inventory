FROM openjdk:13-slim-buster
# Supported JDK versions: https://github.com/h2oai/h2o-3/blob/jenkins-3.28.0.3/docker/Jenkinsfile-build-docker#L21

RUN apt-get update && \
    apt-get install -y wget unzip && \
    mkdir h2o-temp && \
    wget https://h2o-release.s3.amazonaws.com/h2o/rel-yu/3/h2o-3.28.0.3.zip -O h2o-temp/h2o.zip -q && \
    unzip -d h2o-temp/zip h2o-temp/h2o.zip && \
    cp h2o-temp/zip/h2o-3.28.0.3/h2o.jar /opt/h2o.jar && \
    rm -r h2o-temp
