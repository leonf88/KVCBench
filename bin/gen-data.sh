#!/usr/bin/env bash

source conf/config.sh
source basic.sh
setpath
ldfunc
SLAVES_SIZE=`wc -l conf/slaves | awk '{print $1}'`

create_tera(){
  P=$1
  S_DIR="/data/terasort/${P}G-tera"

  create_tera_data "${S_DIR}" "$((P*4/SLAVES_SIZE))" "256"
}

create_text(){
  P=$1
  S_DIR="/data/text/${P}G-text"

  create_text_data "${S_DIR}" "$((P*4/SLAVES_SIZE))" "256"
}

bash $HADOOP_HOME/sbin/start-yarn.sh
#create_text 2
create_tera 100
