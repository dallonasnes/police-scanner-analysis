"""
This script reads in a csv of [zone_timestamp, text]
and reformats for the final source data schema:

id | dept name | zone | time of day | date of event | duration | text
"""
import random
from random import randrange
import uuid
import csv
from datetime import datetime, timedelta


#source: https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

if __name__ == '__main__':
    texts = []
    with open('zone_ts_text.csv', 'r') as file:
        reader = csv.reader(file)
        texts = [row[1] for row in reader if row]

    d1 = datetime.strptime('1/1/2020 12:00 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/5/2020 4:50 AM', '%m/%d/%Y %I:%M %p')

    dept_name = "cpd"
    zones = ["zone1", "zone2", "zone3", "zone4"]

    output = [] #[ ['a', 'b', 'c'], ['d', 'e', 'f'] ]
    for text in texts:
        id = str(uuid.uuid4())
        zone = zones[random.randint(0, len(zones)-1)]
        date, time = str(random_date(d1, d2)).split(" ")
        duration = random.random() * 60.0 #time period btwn 0 and 60 minutes

        row = [id, dept_name, zone, time, date, duration, text]
        output.append(row)

    #now write the new output array to csv, which we'll then move into hdfs as source data
    with open('starter_data_final_schema.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        [writer.writerow(row) for row in output]