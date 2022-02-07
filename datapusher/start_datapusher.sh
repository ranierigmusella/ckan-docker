#!/bin/sh

UWSGI_OPTS="--plugins http,python \
            --socket /tmp/uwsgi.sock \
            --wsgi-file ${APP_DIR}/datapusher.wsgi \
            --uid www-data --gid www-data \
            --http 0.0.0.0:8800 \
            --master --enable-threads \
            --lazy-apps \
            -p 4 -L -b 32768 --vacuum \
            --harakiri $UWSGI_HARAKIRI"
            
uwsgi $UWSGI_OPTS