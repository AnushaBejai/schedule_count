import os
import psycopg2
import uuid
import requests
import json
import uuid
import logging
import traceback

from datetime import timedelta, date, datetime, timezone
    
goal_list=[]

user_database = os.environ.get('task')
db_host = os.environ.get('localhost')
db_user = os.environ.get('postgres')
db_password = os.environ.get('anu@0101')
goal_completion_log_file = os.environ.get('goal_completion_log_file')

# -----------------------------------------------------------------

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

f_handler = logging.FileHandler(goal_completion_log_file)
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)

# -----------------------------------------------------------------



def setup_psql_connection(database):# Postgres database connection
    try:
        conn = psycopg2.connect(
        host=db_host,
        database=database,
        user=db_user,
        password=db_password
        )
    except Exception as err:
        print(str(err))
    return conn.cursor(),conn

cursor,conn = setup_psql_connection(user_database)

def complete_goal():
    try:
        update_goal = "UPDATE user_goals \
        	SET status='COMPLETED' \
        	WHERE to_date(cast(end_date as TEXT),'YYYY-MM-DD') = CURRENT_DATE \
        	AND status='ACTIVE';"
        update_record = ('COMPLETED', )
        logger.info('query')
        logger.info(update_goal)

        cursor.execute(update_goal, update_record)
        conn.commit()
        conn.close()
    except Exception as e:
        conn.rollback()
        logger.error(
                str(e) +"--"+ traceback.format_exc()
            )
        conn.close()

complete_goal()