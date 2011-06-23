from datetime import datetime
from time import mktime
import json
import fileinput
import sys

knmi = []

for line in fileinput.input(sys.argv[1]):
	parts = line.strip().split(',')
	time = datetime.strptime(parts[1], '%Y%m')
	knmi.append([long (mktime(time.timetuple())) * 1000, parts[2]])

print('knmi = ' + json.dumps(knmi) + ';')
