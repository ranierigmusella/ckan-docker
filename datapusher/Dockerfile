FROM alpine:3.13

LABEL  MANTEINER="Ranieri Musella"

ENV APP_DIR=/srv/app
ENV GIT_BRANCH 0.0.17
ENV GIT_URL https://github.com/ckan/datapusher.git
ENV JOB_CONFIG ${APP_DIR}/datapusher_settings.py
ENV DATAPUSHER_MAX_CONTENT_LENGTH 1024000
ENV DATAPUSHER_CHUNK_SIZE 16384
ENV DATAPUSHER_CHUNK_INSERT_ROWS 250
ENV DATAPUSHER_DOWNLOAD_TIMEOUT 30
ENV UWSGI_HARAKIRI 120

WORKDIR ${APP_DIR}

RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-wheel \
    libffi-dev \
    libressl-dev \
    libxslt \
    libmagic \
    uwsgi \
    uwsgi-http \
    uwsgi-corerouter \
    uwsgi-python \
    # Temporary packages to build DataPusher requirements
    && apk add --no-cache --virtual .build-deps \
    gcc \
    git \
    musl-dev \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    libmagic \
    openssl-dev \
    cargo

RUN mkdir ${APP_DIR}/src && cd ${APP_DIR}/src && \
    # The branch 0.0.17 has a commit problem with the settings file. I take the last for now...
    # git clone -b ${GIT_BRANCH} --depth=1 --single-branch ${GIT_URL} && \
    git clone ${GIT_URL} && \
    cd datapusher && \
    python3 setup.py install && \
    pip3 install --no-cache-dir -r requirements.txt


RUN apk del .build-deps && \
    cp ${APP_DIR}/src/datapusher/deployment/*.* ${APP_DIR} && \
    # Remove default values in ini file
    sed -i '/http/d' ${APP_DIR}/datapusher-uwsgi.ini && \
    sed -i '/wsgi-file/d' ${APP_DIR}/datapusher-uwsgi.ini && \
    sed -i '/virtualenv/d' ${APP_DIR}/datapusher-uwsgi.ini && \
    rm -rf ${APP_DIR}/src && \
    rm -rf ${APP_DIR}/datapusher-uwsgi.ini

# Create a local user and group to run the app
RUN addgroup -g 92 -S www-data && \
    adduser -u 92 -h /srv/app -H -D -S -G www-data www-data

COPY ./start_datapusher.sh ${APP_DIR}/
COPY ./datapusher_settings.py ${APP_DIR}/

RUN chmod +x ${APP_DIR}/start_datapusher.sh

RUN chown www-data -R /srv/app

EXPOSE 8800

USER www-data

CMD ["/srv/app/start_datapusher.sh"]
