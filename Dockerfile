FROM python:3.10-alpine

# setup for all users
RUN umask 022

# set working directory
WORKDIR /app

# copy entire directory into container
COPY . /app/dcm-import-module
# move AppConfig
RUN mv /app/dcm-import-module/app.py /app/app.py

# install app package
RUN pip install --upgrade \
    --extra-index-url https://zivgitlab.uni-muenster.de/api/v4/projects/9020/packages/pypi/simple \
    "/app/dcm-import-module/[cors]"
RUN rm -r dcm-import-module/

# install wsgi server
RUN pip install gunicorn

# add and set default user
RUN adduser -u 303 -S dcm -G users
RUN mkdir -p /file_storage && chown -R dcm:users /file_storage && chmod -R +w /file_storage
USER dcm

# define startup
ENV WEB_CONCURRENCY=5
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:80 --workers 1 --threads ${WEB_CONCURRENCY} app:app"]
