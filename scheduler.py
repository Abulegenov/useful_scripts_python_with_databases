import time
import schedule

def Start_Job():
    #Some operation to be processed regularly
    pass

schedule.clear()
schedule.every().day.at("09:30").do(Start_Job)

#Scheduler to run the job every day at 9.30 a.m
while True:
    schedule.run_pending()
    time.sleep(1)