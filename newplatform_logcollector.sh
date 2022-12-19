#!/bin/bash
set -e

CUR=/opt/td-agent/logcollector
YESTERDAY=`date -d "yesterday 13:00" '+%Y%m%d'`
YESTERDAY1=`date -d "yesterday 13:00" '+%Y-%m-%d'`
TWO_DAY_AGO=`date -d "2day ago 13:00" '+%Y%m%d'`
TWO_DAY_AGO1=`date -d "2day ago 13:00" '+%Y-%m-%d'`

SRC_ACT_DIR=/data/do_log/go_user/activity_$YESTERDAY.log
SRC_MAIL_DIR=/data/do_log/webmail/$YESTERDAY.log
SRC_WEB_ADMIN=/data/do_log/catalina/webadmin/access_$YESTERDAY1.txt
SRC_WEB_MAIL=/data/do_log/catalina/webmail/access_$YESTERDAY1.txt

DEST_ACT_LOGS=$CUR/newplatform/activitylog/ap*.daouoffice.com_activity_$YESTERDAY.log
DEST_MAIL_LOGS=$CUR/newplatform/maillog/ap*.daouoffice.com_$YESTERDAY.log
DEST_WEB_ADMIN_LOGS=$CUR/newplatform/access_admin/ap*.daouoffice.com_access_$YESTERDAY1.txt
DEST_WEB_MAIL_LOGS=$CUR/newplatform/access_mail/ap*.daouoffice.com_access_$YESTERDAY1.txt
DEST_DB_LOGS=$CUR/newplatform/org_db/do_user_bigdata_tf_$YESTERDAY.csv
LOG_FILE=$CUR/log_collector.log

ERROR_ARR=("")

log(){
  echo "$1" >> $LOG_FILE
}

init(){
  if [ -e $CUR/newplatform/activitylog/ap1.daouoffice.com_activity_$TWO_DAY_AGO.log ]; then
    rm $CUR/newplatform/activitylog/ap*.log
    sleep 5
    echo "[activity] Delete existing file"
  else
    echo "[activity] File not exist"
  fi

  if [ -e $CUR/newplatform/maillog/ap1.daouoffice.com_$TWO_DAY_AGO.log ]; then
    rm $CUR/newplatform/maillog/ap*.log
    sleep 3
    echo "[maillog] Delete existing file"
  else
    echo "[maillog] File not exist"
  fi

  if [ -e $CUR/newplatform/access_mail/ap1.daouoffice.com_access_$TWO_DAY_AGO1.txt ]; then
    rm $CUR/newplatform/access_mail/ap*.txt
    sleep 3
    echo "[webmail] Delete existing file"
  else
    echo "[webmail] File not exist"
  fi


  if [ -e $CUR/newplatform/access_admin/ap-admin1.daouoffice.com_access_$TWO_DAY_AGO1.txt ]; then
    rm $CUR/newplatform/access_admin/ap*.txt
    sleep 3
    echo "[webadmin] Delete existing file"
  else
    echo "[webadmin File not exist"
  fi

  echo "initialize complete"

}

download_webmail(){
  echo "[webmail] source ---> dest copy"
  if [ -e $SRC_WEB_MAIL ]; then
    cp $SRC_WEB_MAIL/ap* $CUR/newplatform/access_mail/
    chown -R td-agent:td-agent $DEST_WEB_MAIL_LOGS
    echo "[webmail] Copy $SRC_WEB_MAIL to $DEST_WEB_MAIL_LOGS"
  else
    ERROR_ARR+="[webmail] File not exist\n"
    echo "[webmail] File not exist"
  fi
}

download_webadmin(){
  echo "[webadmin] source ---> dest copy"
  if [ -e $SRC_WEB_ADMIN ]; then
    cp $SRC_WEB_ADMIN/ap* $CUR/newplatform/access_admin/
    chown -R td-agent:td-agent $DEST_WEB_ADMIN_LOGS
    echo "[webadmin] Copy $SRC_WEB_ADMIN to $DEST_WEB_ADMIN_LOGS"
  else
    ERROR_ARR+="[webadmin] File not exist\n"
    echo "[webadmin] File not exist"
  fi
}

download_mail(){
  echo "[mail] source ---> dest copy"
  if [ -e $SRC_MAIL_DIR ]; then
    echo "[mail] Copy..."
    cp $SRC_MAIL_DIR/ap* $CUR/newplatform/maillog/
    chown -R td-agent:td-agent $DEST_MAIL_LOGS
    echo "[mail] Copy complete"
  else
    ERROR_ARR+="[mail] File not exist\n"
    echo "[mail] File not exist"
  fi
}

download_activity(){
  echo "[activity] source ---> dest copy"
  if [ -e $SRC_ACT_DIR ]; then
    echo "[activity] Copy..."
    cp $SRC_ACT_DIR/ap* $CUR/newplatform/activitylog/
    chown -R td-agent:td-agent $DEST_ACT_LOGS
    echo "[activity] Copy complete"
  else
    ERROR_ARR+="[activity] File not exist\n"
    echo "[activity] File not exist"
  fi
}

####################################################################
#  main
####################################################################

echo "============================================="
echo " Actlog Scheduler"
echo "    - executed on `date '+%Y.%m.%d %R:%S'`"
echo "============================================="

init
download_activity
#download_mail
#download_webadmin
#download_webmail
sed "1d" $CUR/do_user_bigdata_$YESTERDAY.csv > $CUR/newplatform/org_db/do_user_bigdata_tf_$YESTERDAY.csv
chown -R td-agent:td-agent $DEST_DB_LOGS

if [[ ${ERROR_ARR[*]} == "" ]]; then
  echo '{"complete": 1, "description": "Finished! '`date +%Y.%m.%d %R:%S`'"}'
else
  echo '{"complete": -1, "code": 10, "description": "'${ERROR_ARR[*]}'"}'
fi

