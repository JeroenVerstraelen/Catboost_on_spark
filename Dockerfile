FROM vito-docker-private.artifactory.vgt.vito.be/python38-hadoop:latest

#ENV VIRTUAL_ENV=/opt/venv
#RUN python3 -m venv $VIRTUAL_ENV
#ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install dependencies:
RUN yum install gcc-c++ -y && yum clean all
COPY requirements.txt .
RUN pip3 install -r requirements.txt
