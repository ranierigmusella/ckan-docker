FROM ranierimusella/ckan:ckan-base-2.9.5 as ckan-with-extensions

LABEL  MANTEINER="Ranieri Musella"

ENV CKAN_DIR=${SRC_DIR}/ckan
# Set timezone
ARG TZ=UTC
RUN echo $TZ > /etc/timezone
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime

## Cloudstorage Extension
## The reuired dependency is deprecated and it doesen't work well with all extensions
# ENV CLOUD_STORAGE_GIT_BRANCH=py3
# COPY ./settings/cloudstorage_settings.txt ${APP_DIR}/
# RUN pip install azure-storage && \ 
#     pip install -e git+https://github.com/ranierigmusella/ckanext-cloudstorage.git@${CLOUD_STORAGE_GIT_BRANCH}#egg=ckanext-cloudstorage && \
#     ckan config-tool ${CKAN_INI} -s app:main -f cloudstorage_settings.txt
# RUN rm -f ${APP_DIR}/cloudstorage_settings.txt

## XLoader Extension
COPY ./settings/xloader_settings.txt ${APP_DIR}/
RUN pip install ckanext-xloader && \
    pip install -r https://raw.githubusercontent.com/ckan/ckanext-xloader/master/requirements.txt && \
    pip install -U requests[security] && \
    ckan config-tool ${CKAN_INI} -s app:main -f xloader_settings.txt
RUN rm -f ${APP_DIR}/xloader_settings.txt

## OData Endpoint Extension

RUN pip install -e git+https://github.com/ranierigmusella/ckanext-odata.git@dgm-ckan2.9#egg=ckanext-odata
COPY ./file_patch/api_info.html ${CKAN_DIR}/ckanext/datastore/templates/ajax_snippets/

## Spatial Extension

## Harvester Extension - Required by the Spatial Extension

RUN apk add --no-cache redis && \
    pip install -e git+https://github.com/ckan/ckanext-harvest.git#egg=ckanext-harvest && \
    pip install -r ${SRC_DIR}/ckanext-harvest/pip-requirements.txt && \
    ckan config-tool ${CKAN_INI} -s app:main "ckan.harvest.mq.type = redis"
COPY ./settings/ckan.harvester.jobs.worker.supervisor.conf /etc/supervisord.d/ckan.harvester.jobs.worker.supervisor.conf

RUN apk add --no-cache \ 
    geos \
    geos-dev \
    proj-util \
    proj-dev && \
    pip install ckantoolkit && \
    pip install -e "git+https://github.com/ckan/ckanext-spatial.git#egg=ckanext-spatial" && \
    pip install -r ${SRC_DIR}/ckanext-spatial/pip-requirements.txt && \
    ckan config-tool ${CKAN_INI} -s app:main "ckanext.spatial.search_backend = solr" "ckan.spatial.srid = 4326"

COPY ./file_patch/search.html ${CKAN_DIR}/ckan/templates/package/
COPY ./file_patch/read_base.html ${CKAN_DIR}/ckan/templates/package/

## Geoview Extension
RUN git clone https://github.com/ckan/ckanext-geoview.git ${SRC_DIR}/ckanext-geoview && \
    pip install -r ${SRC_DIR}/ckanext-geoview/pip-requirements.txt && \
    cd ${SRC_DIR}/ckanext-geoview/ && \
    python setup.py install && \
    python setup.py develop && \
    ckan config-tool ${CKAN_INI} -s app:main "ckanext.geoview.geojson.max_file_size = 3221225472" "ckan.views.default_views = image_view text_view recline_view geojson_view wmts_view shp_view" && \
    cd ${APP_DIR}

## DCAT Extension
RUN pip install -e git+https://github.com/ckan/ckanext-dcat.git#egg=ckanext-dcat && \
    pip install -r ${SRC_DIR}/ckanext-dcat/requirements.txt

## Site Search Extension
RUN pip install -e git+https://github.com/okfn/ckanext-sitesearch.git#egg=ckanext-sitesearch && \
    pip install -r ${SRC_DIR}/ckanext-sitesearch/requirements.txt

## Tag-Taxonomy Extension
## ckan -c ckan.ini taxonomy load --filename /srv/app/files_to_be_copied/UNDP_Capacity_Tag_Taxonomy.rdf --name "NGDP Capacity" --title "NGDP Capacity" --uri "http://data.undp.org/taxonomies/capacity/version01"

RUN pip install -e git+https://github.com/ranierigmusella/python-skos.git#egg=python-skos && \
    cd ${SRC_DIR} && \
    git clone https://github.com/ranierigmusella/ckanext-taxonomy.git && \
    cd ./ckanext-taxonomy && \
    python setup.py install

## Stats Extension

RUN ckan config-tool ${CKAN_INI} -s app:main "ckanext.stats.cache_enabled = true" 

## Page View Tracking Extension

RUN ckan config-tool ${CKAN_INI} -s app:main "ckan.tracking_enabled = true"

## SAML2 Authentication Extension

COPY ./settings/saml2auth_settings.txt ${APP_DIR}/
RUN apk add --no-cache xmlsec && \
    pip install ckanext-saml2auth && \
    ckan config-tool ${CKAN_INI} -s app:main -f saml2auth_settings.txt && \
    rm -f ${APP_DIR}/saml2auth_settings.txt

## Redis in Azure fix

RUN mv ${CKAN_DIR}/ckan/lib/redis.py ${CKAN_DIR}/ckan/lib/redis_official.py
COPY ./file_patch/redis.py ${CKAN_DIR}/ckan/lib/

## Cronjobs setup

RUN pip install yacron
COPY ./settings/ckan.yacron.jobs.config.yaml ${APP_DIR}/
COPY ./settings/ckan.yacron.jobs.worker.supervisor.conf /etc/supervisord.d/ckan.yacron.jobs.worker.supervisor.conf

## CKAN config specific settings

RUN ckan config-tool ${CKAN_INI} -s app:main "ckan.resource_proxy.max_file_size = 3221225472"

## Copy any necessary files to the image

RUN mkdir ${APP_DIR}/files_to_be_copied && \
    chown ckan:ckan ${APP_DIR}/files_to_be_copied

COPY files_to_be_copied ${APP_DIR}/files_to_be_copied

FROM ckan-with-extensions

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
