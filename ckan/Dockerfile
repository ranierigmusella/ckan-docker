FROM ranierimusella/ckan:ckan-base-2.9.4 as ckan-with-extensions

LABEL  MANTEINER="Ranieri Musella"

ENV CKAN_DIR=${SRC_DIR}/ckan
# Set timezone
ARG TZ=UTC
RUN echo $TZ > /etc/timezone
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime

## Cloudstorage Extension
ENV CLOUD_STORAGE_GIT_BRANCH=py3
COPY cloudstorage_settings.txt ${APP_DIR}/
RUN pip install azure-storage && \ 
    pip install -e git+https://github.com/ranierigmusella/ckanext-cloudstorage.git@${CLOUD_STORAGE_GIT_BRANCH}#egg=ckanext-cloudstorage && \
    ckan config-tool ${CKAN_INI} -s app:main -f cloudstorage_settings.txt
RUN rm -f ${APP_DIR}/cloudstorage_settings.txt

## XLoader Extension
COPY xloader_settings.txt ${APP_DIR}/
RUN pip install ckanext-xloader && \
    pip install -r https://raw.githubusercontent.com/ckan/ckanext-xloader/master/requirements.txt && \
    pip install -U requests[security] && \
    ckan config-tool ${CKAN_INI} -s app:main -f xloader_settings.txt
RUN rm -f ${APP_DIR}/xloader_settings.txt

## OData Endpoint Extension
RUN pip install -e git+https://github.com/ranierigmusella/ckanext-odata.git#egg=ckanext-odata

## Spatial Extension

## Harvester Extension - Required by the Spatial Extension
## https://github.com/ckan/ckanext-harvest/ --> Complete the Production setup from point 4.

RUN apk add --no-cache redis && \
    pip install -e git+https://github.com/ckan/ckanext-harvest.git#egg=ckanext-harvest && \
    pip install -r ${SRC_DIR}/ckanext-harvest/pip-requirements.txt && \
    ckan config-tool ${CKAN_INI} -s app:main "ckan.harvest.mq.type = redis"
COPY ./harvester.worker.conf /etc/supervisord.d/harvester.worker.conf
COPY ./harvester.cronjob.sh /etc/periodic/15min/

RUN apk add --no-cache \ 
    geos \
    geos-dev \
    proj-util \
    proj-dev 

RUN pip install ckantoolkit
RUN pip install -e "git+https://github.com/ckan/ckanext-spatial.git#egg=ckanext-spatial" && \
    pip install -r ${SRC_DIR}/ckanext-spatial/pip-requirements.txt && \
    ckan config-tool ${CKAN_INI} -s app:main "ckanext.spatial.search_backend = solr" "ckan.spatial.srid = 4326"

## Geoview Extension
RUN git clone https://github.com/ckan/ckanext-geoview.git ${SRC_DIR}/ckanext-geoview && \
    pip install -r ${SRC_DIR}/ckanext-geoview/pip-requirements.txt && \
    cd ${SRC_DIR}/ckanext-geoview/ && \
    python setup.py install && \
    python setup.py develop && \
    ckan config-tool ${CKAN_INI} -s app:main "ckanext.geoview.geojson.max_file_size = 3221225472" "ckan.views.default_views = image_view text_view recline_view geo_view geojson_view wmts_view shp_view" && \
    cd ${APP_DIR}

# redis in Azure fix
RUN mv ${CKAN_DIR}/ckan/lib/redis.py ${CKAN_DIR}/ckan/lib/redis_official.py
COPY redis.py ${CKAN_DIR}/ckan/lib/

FROM ckan-with-extensions

## Tag-Taxonomy Extension
# RUN cd ${SRC_DIR} && \
#     git clone https://github.com/ranierigmusella/ckanext-taxonomy.git && \
#     cd ./ckanext-taxonomy && \
#     python setup.py install
# RUN ckan -c ${CKAN_INI} taxonomy init

# Apply any patches needed to CKAN core or any of the built extensions (not the
# runtime mounted ones)
# See https://github.com/okfn/docker-ckan#applying-patches
COPY patches ${APP_DIR}/patches

RUN for d in $APP_DIR/patches/*; do \
        if [ -d $d ]; then \
            for f in `ls $d/*.patch | sort -g`; do \
                cd $SRC_DIR/`basename "$d"` && echo "$0: Applying patch $f to $SRC_DIR/`basename $d`"; patch -p1 < "$f" ; \
            done ; \
        fi ; \
    done

RUN chown -R ckan:ckan $CKAN_STORAGE_PATH && \
    chown -R ckan:ckan /srv/app

USER ckan
