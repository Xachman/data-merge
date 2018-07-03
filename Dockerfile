FROM python:3.6.1-alpine


RUN mkdir /usr/src/app && apk add --no-cache gcc musl-dev libffi-dev openssl-dev \
&& pip install PyMySQL
WORKDIR /usr/src/app



CMD python app.py