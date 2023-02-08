FROM python:3.10-slim

ENV USER_UID=10001 \
    USER_NAME=afm

COPY  build/bin /usr/local/bin
RUN  /usr/local/bin/user_setup

COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY main.py main.py
COPY afm afm

RUN mkdir -p /etc/plugins
RUN chown afm /etc/plugins

EXPOSE 8080

ENTRYPOINT ["python3"]
CMD ["main.py", "--loglevel", "trace"]

USER ${USER_UID}

