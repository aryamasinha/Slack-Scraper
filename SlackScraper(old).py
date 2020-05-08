import requests as r
import json
import pandas as pd
import datetime
import time
startdate = datetime.datetime()
enddate = datetime.datetime()
startdateunix = str(time.mktime(startdate.timetuple()))
enddateunix = str(time.mktime(enddate.timetuple()))
token = ''
channelID = ''
url = 'https://slack.com/api/channels.history?token='+token+'&channel='+channelID+'&count=1000&latest='+enddateunix+'&oldest='+startdateunix+'&pretty=1'
file = r.get(url)
jsonfile = json.loads(file.content)
slackdata = jsonfile['messages']
df = pd.DataFrame.from_dict(slackdata, orient = 'columns')
text_ts = df.filter(items = ['text','ts'])
text_ts.to_csv('publishtimedata_'+str(startdate)+'.csv')


