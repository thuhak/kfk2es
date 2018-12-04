FROM docker.io/python
WORKDIR /
COPY __init__.py kfk2es/
COPY kfk2es.py kfk2es/
COPY myconf/__init__.py kfk2es/myconf/
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
MAINTAINER thuhak.zhou@nio.com
