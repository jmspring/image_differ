FROM python:2.7-alpine

ENV INSTALL_PATH /app
RUN mkdir -p $INSTALL_PATH

WORKDIR $INSTALL_PATH

ADD requirements.txt /tmp/requirements.txt

RUN apk add --no-cache --virtual .build-deps \
    build-base libffi-dev openssl-dev \
  	&& pip install -r /tmp/requirements.txt \
	&& rm /tmp/requirements.txt \
	&& apk del .build-deps \
	&& apk add --no-cache py-pillow bash \
	&& cp -rp /usr/lib/python2.7/site-packages/PIL /usr/local/lib/python2.7/site-packages/ \
	&& cp -rp /usr/lib/python2.7/site-packages/Pillow* /usr/local/lib/python2.7/site-packages/ \
	&& cp -rp /usr/bin/pil*.py /usr/local/bin/

COPY . . 

EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["image_differ.py"]
