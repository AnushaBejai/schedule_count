import os
import psycopg2
import uuid
import requests
import json
import uuid
import logging
import traceback
from calendar import monthrange
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from datetime import timedelta, date, datetime, timezone
    
goal_list=[]
schedule_count = []






user_database = os.environ.get('user_database')
db_host = os.environ.get('db_host')
db_user = os.environ.get('db_user')
db_password = os.environ.get('db_password')
create_payment_scheduler_log_file = os.environ.get('create_payment_scheduler_log_file')

# -----------------------------------------------------------------

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

f_handler = logging.FileHandler(create_payment_scheduler_log_file)
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


def get_goal_payment_details():
    '''
    '''

    qry = f"select sq.user_goal, sq.schedule_count, \
        sq.payment_date, ug2.monthly_payment, ug2.payment_frequency, \
        ug2.end_date, ug2.user, upd.payment_day, upd.payment_order \
        from \
        ( \
            select user_goal, max(payment_date) payment_date,count(ups.id) schedule_count \
            from user_payment_schedule ups, user_goals ug \
            where ug.status='ACTIVE' and ug.id=ups.user_goal and \
            ups.payment_status in \
            (select id from user_payment_schedule_status where status='scheduled') \
            group by user_goal \
                    ) as sq , user_goals ug2 , \
            user_payment_dates upd \
        where sq.schedule_count<=5 and \
        ug2.id=sq.user_goal and upd.user_goal=ug2.id"


    
    logger.info('fetch details query')
    logger.info(qry)

    cursor.execute(qry)

   

    goal_records = cursor.fetchall()

    logger.info('goal records')
    logger.info(goal_records)

    #fetching schedule count list
    for i in range(0,len(goal_records)):
            x=goal_records[i]
            schedule_each_count=x[1]
            schedule_count.append(schedule_each_count)


    column_names = [column[0] for column in cursor.description]
    goal_data = []
    for record in goal_records:
        goal_data.append(dict(zip(column_names, record)))

    logger.info('goal_data')
    logger.info(goal_data)
    
    user_goal_key_value_pair = {}
    for goal_record in goal_data:
        if goal_record['user_goal'] in user_goal_key_value_pair:
            user_goal_key_value_pair[goal_record['user_goal']].append(
                goal_record)
        else:
            user_goal_key_value_pair[goal_record['user_goal']] = [goal_record]
    
    logger.info('user_goal_key_value_pair')
    logger.info(user_goal_key_value_pair)

    structured_data_for_payment = []

    for goal_id, details in user_goal_key_value_pair.items():
        goal_details = {}
        goal_details["payment_dates"] = [
                {
                    "payment_day": payment['payment_day'],
                    "payment_order": payment['payment_order']
                }
                for payment in details
            ]
        goal_details["payment_frequency"] = details[0]['payment_frequency']
        goal_details["last_payment_date"] = details[0]['payment_date']
        goal_details["monthly_payment"] = details[0]['monthly_payment']
        goal_details["goal_id"] = goal_id
        goal_details['user'] = details[0]['user']
        goal_details['end_date'] = details[0]['end_date']
        structured_data_for_payment.append(goal_details)

    logger.info('structured_data_for_payment before payment date insertion')
    logger.info(structured_data_for_payment)

    logger.info('structured_data_for_payment after payment date insertion')
    logger.info(structured_data_for_payment)

    return structured_data_for_payment
    print("")
    


def create_payment_shedules(data):
    '''
    '''

    insert_schedule_query = f"insert into user_payment_schedule\
        (\"id\",\"user\",user_goal,payment_date,transaction_type,amount,created_at, updated_at, payment_status) values\
        "
    insert_history_query = f"insert into user_payment_schedule_status_history \
        (id, timestamp, user_payment_schedule_id, schedule_status_id, description) values"

    #subtracting 6 from schedule_count items
    next_count=[]
    for count in schedule_count:
        upcoming_count = 6 - count 
        next_count.append(upcoming_count)

    considered_dates = []


#looping the schedule_counts subtracted by 6
    for goal_details in data:
        for count in next_count:
            id = uuid.uuid4()
            for i in range(count):
                user = goal_details['user']
                user_goal = goal_details['goal_id']
                payment_date = get_next_payment(
                        goal_details['last_payment_date'],
                        goal_details['payment_frequency'],
                        goal_details['payment_dates']
                    )
                goal_details['last_payment_date'] = payment_date
                transaction_type = "sip"
                if goal_details['payment_frequency'] == "once_per_month":
                    amount = goal_details['monthly_payment']
                else:
                    amount = goal_details['monthly_payment']/2
                status = get_scheduled_status()
                if payment_date < goal_details['end_date']:
                    considered_dates.append(payment_date)
                    insert_schedule_query += f"(\'{id}\',\'{user}\',\'{user_goal}\',\'{payment_date}\',\
                        \'{transaction_type}\', {amount}, current_timestamp, current_timestamp, \'{status}\'),"

                    insert_history_query += f"('{id}', current_timestamp, '{id}', '{status}', 'payment scheduled' ),"
        
    logger.info('insert_schedule_query')
    logger.info(insert_schedule_query)

    logger.info('insert_history_query')
    logger.info(insert_history_query)

    if considered_dates:
        cursor.execute(insert_schedule_query[:-1])
        cursor.execute(insert_history_query[:-1])
        conn.commit()


def get_next_payment(last_payment_date, payment_frequency, payment_dates):

    if payment_frequency == "once_per_month":
        next_payment_date = last_payment_date + relativedelta(
            months=1
        )
        try:
            next_payment_date = next_payment_date.replace(
                day=payment_dates[0]['payment_day']
            )
        except ValueError:
            next_payment_date = next_payment_date.replace(
                day=monthrange(
                    next_payment_date.year, next_payment_date.month
                )[1]
            )
    elif payment_frequency == "twice_per_month":
        payment_date = last_payment_date.day
        executed_payment = [
            payment_order for payment_order 
            in payment_dates 
            if payment_order['payment_day']==payment_date]
        if not executed_payment:
            executed_payment = payment_dates[1]
        else:
            executed_payment = executed_payment[0]

        if executed_payment['payment_order'] == 1:
            try:
                next_payment_date = last_payment_date.replace(
                    day=payment_dates[1]['payment_day']
                )
            except ValueError:
                next_payment_date = last_payment_date.replace(
                    day=monthrange(
                        last_payment_date.year, last_payment_date.month
                    )[1]
                )
        else:
            next_payment_date = last_payment_date + relativedelta(
                months=1
            )
            try:
                next_payment_date = next_payment_date.replace(
                    day=payment_dates[0]['payment_day']
                )
            except ValueError:
                next_payment_date = next_payment_date.replace(
                    day=monthrange(
                        next_payment_date.year, next_payment_date.month
                    )[1]
                )
    return next_payment_date    


def get_scheduled_status():
    '''
    '''
    qry = "select id from user_payment_schedule_status where status='scheduled'"
    cursor.execute(qry)
    data = cursor.fetchall()
    data = data[0]
    return data[0]


def create_payment_scheduler():
    '''
    '''
    try:
        goal_payment_details = get_goal_payment_details()
        create_payment_shedules(goal_payment_details)
    except Exception as e:
        conn.rollback()
        logger.error(
                str(e) +"--"+ traceback.format_exc()
            )
    finally:
        conn.close()




create_payment_scheduler()




