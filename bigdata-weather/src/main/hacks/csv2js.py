from datetime import datetime
from time import mktime
import json
import fileinput
import sys

knmi = {}

for line in fileinput.input(sys.argv[1]):
    parts = line.strip().split(',')

    station = parts[0]
    time = datetime.strptime(parts[1], '%Y%m')
    percipitation = int(parts[2])

    if station not in knmi:
        knmi[station] = []

    knmi[station].append([long (mktime(time.timetuple())) * 1000, percipitation])

output = []

for station in knmi.keys():
    record = { 'data': knmi[station],
               'label': station }
    output.append(record)

print 'knmi = ' + json.dumps(output) + ';'
