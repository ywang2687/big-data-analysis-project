import pandas as pd
import matplotlib.pyplot as plt
import os
import json

latest_data = {}

for i in range(4):
    filename = f"/files/partition-{i}.json"
   # print(filename)
    if not os.path.exists(filename):
        continue
    elif os.path.exists(filename):
        with open(filename, 'r') as file:
            data = json.load(file)
           # print(data)
        for month in ['January', 'February', 'March']:
            if month in data:
               #print(month)
                for year, details in data[month].items():
                   # print(details)
                    #print(year)
                   # max_year = max(year)
                   # print(max_year)
                   # print(data[month].keys())
                    for i in data[month].keys():
                        max_key = int(max( data[month], key = int))
                       # print(int(i), max_key)
                        if int(i)== max_key:
                            print(int(i))
                            m_d = f"{month}-{max_key}"
                            latest_data[m_d] = {'year': max_key, 'temp': details['avg']}
                       # print(latest_data)

month_series = pd.Series({month: data['temp'] for month, data in latest_data.items()})

fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("/files/month.svg")
