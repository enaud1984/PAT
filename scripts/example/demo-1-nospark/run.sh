#!/bin/sh

export  =/data/01/cloudera_software/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/superset/bin/python3.6
export ENABLE_PRINT=ON
export CLASSPATH=/home/ec2-user/script/common/ImpalaJDBC41.jar:/home/ec2-user/script/common/postgresql-42.2.14.jar:$CLASSPATH


spark-submit \
 --master yarn\
 --deploy-mode client \
 --jars /home/ec2-user/script/common/postgresql-42.2.14.jar,/home/ec2-user/script/common/ImpalaJDBC41.jar \
 --driver-class-path /home/ec2-user/script/common/postgresql-42.2.14.jar,/home/ec2-user/script/common/ImpalaJDBC41.jar \
 --executor-memory 3G \
 --driver-memory 2G \
script.py

