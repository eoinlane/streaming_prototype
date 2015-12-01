# event_generator.py - a program to simulate retail events going to a Kafka queue
# arguments:
#    EventFrequencyPerDay = number of events to be generated for each date
#    StartDate = date of first event
#    EndDate = date of last event
#    Delay = if Y then events will be generated with a timed delay, if N then events will be created all at once
import sys
import datetime
import time
import string
import math
import random
import numpy.random
import json
from pykafka import KafkaClient

# section to receive and check command line arguments
if len(sys.argv) != 5:
    sys.exit('Error(s) in inputs.\n Usage: py event_generator.py EventFrequencyPerDay StartDate EndDate Delay \n EventFrequencyPerDay = 1 to 86400 \n Date Format = yyyy-mm-dd \n Delay = Y for timed release, N otherwise')
EventFrequencyPerDay = int(sys.argv[1])
StartDate = sys.argv[2]
EndDateStr = sys.argv[3]
Delay = sys.argv[4]
if Delay in ('y','yes','Yes','YES','Y'):
    Delay = 'Y'
else:
    Delay = 'N'

syear = int(StartDate[0:4])
smonth = int(StartDate[5:7])
sday = int(StartDate[8:10])
dropout = False
# initialize date values
EventDate = datetime.date.today()
EndDate = datetime.date.today()

try:
    EventDate = datetime.date(syear,smonth,sday)
except OSError as err:
    print("OS error: {0}".format(err))
    dropout = True
except ValueError as err:
    print("StartDate conversion error: {0}".format(err))
    dropout = True
except:
    print("Unexpected error:", sys.exc_info()[0])
    dropout = True
    raise

eyear = int(EndDateStr[0:4])
emonth = int(EndDateStr[5:7])
eday = int(EndDateStr[8:10])

try:
    EndDate = datetime.date(eyear,emonth,eday)
except OSError as err:
    print("OS error: {0}".format(err))
    dropout = True
except ValueError as err:
    print("EndDate conversion error: {0}".format(err))
    dropout = True
except:
    print("Unexpected error:", sys.exc_info()[0])
    dropout = True
    raise

if EventDate > EndDate:
    print('End Date is before Start Date.')
    dropout = True
if EventFrequencyPerDay > 86400 :
    print ('Daily frequency of ', EventFrequencyPerDay, ' is too high.  Maximum daily frequency is 86400.  Reduce and try again.')
    dropout = True
elif EventFrequencyPerDay <= 0:
    print ('Specify daily frequency in the range of 1 to 86400.')
    dropout = True

if dropout :
    sys.exit('Error(s) in inputs.\n Usage: py event_generator.py EventFrequencyPerDay StartDate EndDate Delay \n EventFrequencyPerDay = 1 to 86400 \n Date Format = yyyy-mm-dd \n Delay = Y for timed release')

secondgap = round((60 * 60 * 24) / EventFrequencyPerDay )

fn = open('sales_orders.txt','w')
EventType = 'SalesOrder'
#open kafka
client = KafkaClient(hosts="server7.bigdata.ibm.com:9092")
print client.topics
topic = client.topics['event']
producer = topic.get_producer()

# loop through the days, then for each day, loop through the events
while EventDate <= EndDate:
    for i in range(EventFrequencyPerDay):
        Customer = random.randrange(100)
#        Customer = random.randrange(5)
        Product = random.randrange(10)
#        Product = random.randrange(3)
        Qty = random.randrange(100)
        UnitAmt  = round(numpy.random.triangular(50,100,300),2)
        PurchaseHour = int(math.floor((i * secondgap) / 3600))
        PurchaseMinute = int(math.floor(math.fmod((i * secondgap) , 3600) / 60))
        PurchaseSecond = int(math.fmod((i * secondgap),60))
        StringDate = datetime.date.__str__(EventDate)
#        msg = '{Customer:' + str(Customer) + ',Product:' + str(Product) + ',Qty:' + str(Qty) + ',UnitAmt:' + str(UnitAmt) + ',PurchaseDate:' + StringDate + 'T' + str(PurchaseHour).zfill(2) + ':' + str(PurchaseMinute).zfill(2) + ':' + str(PurchaseSecond).zfill(2) + 'Z}'
#        print(msg)
#        fn.write(msg)
#        json.dump({"Customer":Customer,"Product":Product, "Qty":Qty, "UnitAmt":UnitAmt,"XactionDate": StringDate + 'T' + str(PurchaseHour) + ':' + str(PurchaseMinute) + ':' + str(PurchaseSecond) + 'Z'},fn,sort_keys=True)
        encodedjson = json.dumps({"Customer":Customer,"Product":Product, "Qty":Qty, "UnitAmt":UnitAmt,"XactionDate": StringDate + 'T' + str(PurchaseHour).zfill(2) + ':' + str(PurchaseMinute).zfill(2) + ':' + str(PurchaseSecond).zfill(2) + 'Z'})
        print encodedjson
        producer.produce([encodedjson])
        fn.write('\n')
        if Delay == 'Y':
            print('Going to sleep for ', secondgap, ' seconds...')
            time.sleep(secondgap)
    EventDate = EventDate + datetime.timedelta(days=1)
fn.close()

# Send Event
python event_generator.py 86400 2015-08-01 2015-08-01 N
