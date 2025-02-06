from datetime import date, timedelta,datetime
from dateutil import relativedelta
import pandas.io.sql as psql


def take_table_info_from_last_months(month_delta, connection):
    """Get the data from the last N months from your table"""
    today = date.today()
    current_day = today - timedelta(today.day-1)
    inter = current_day - relativedelta.relativedelta(months=month_delta)
    previous_day = inter - timedelta(1)
    print(today, current_day, previous_day)
    df_last_n_months = psql.read_sql(f"""SELECT * FROM table
                                            WHERE transaction_date>'{previous_day}' and trans_date < '{current_day}'""", connection)
    return df_last_n_months

def last_day_of_next_months(month_delta):
    """get the last day of any next months starting from current month"""
    today = date.today()
    next_month_day = today + relativedelta.relativedelta(months=month_delta)
    last_day_month = next_month_day - timedelta(next_month_day.day)
    return last_day_month.day

today = date.today()

#Below is the query example
table_7_query = f"""
select client, 
extract(month from transaction_date) as month, 
city, age_gender_group, 
case
when toDayOfWeek(transaction_date) = 1 then 'Monday'
when toDayOfWeek(transaction_date) = 2 then 'Tuesday'
when toDayOfWeek(transaction_date) = 3 then 'Wednesday'
when toDayOfWeek(transaction_date) = 4 then 'Thursday'
when toDayOfWeek(transaction_date) = 5 then 'Friday'
when toDayOfWeek(transaction_date) = 6 then 'Saturday'
when toDayOfWeek(transaction_date) = 7 then 'Sunday'
end as week_day,
case 
when extract(hour from transaction_date)> 22 or extract(hour from transaction_date)<=7 then 'Night (22.00-06.59)'
when extract(hour from transaction_date)> 7 and extract(hour from transaction_date)<=12 then 'Morning (07.00-11.59)'
when extract(hour from transaction_date)> 12 and extract(hour from transaction_date)<=18 then 'Day (12.00-17.59)'
when extract(hour from transaction_date)> 18 or extract(hour from transaction_date)<=22 then 'Evening (18.00-21.59)'
end as day_time,
sic_code,
sum(local_amount) as sum, 
count(*) as transaction_amount

from table
where  

transaction_date >= '{today}' and trans_date<='{last_day_of_next_months(3)}'
and trans_country = 'KAZ'
and sic_code in (list_of_sic_codes)
group by client, month, age_gender_group, sic_code, city, week_day, day_time 

"""