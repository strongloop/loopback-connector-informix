FROM ibmcom/informix-innovator-c

ADD ../test/tables.sql /home/informix/tables.sql
ADD ./inf_create.sh /home/informix/inf_create.sh
ADD ./informix_start.sh /

ENV LICENSE=accept

RUN sudo chown informix:informix /home/informix/tables.sql
RUN sudo chown informix:informix /home/informix/inf_create.sh
RUN sudo chmod ug+x /home/informix/inf_create.sh

ENV CLIENT_LOCALE=en_US.utf8
ENV DB_LOCALE=en_US.utf8
ENV GL_USEGLU=1
