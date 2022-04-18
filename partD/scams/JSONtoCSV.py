import json

with open('scams.json') as json_file:
    data = json.load(json_file)

out_file = open('scams.csv', 'w')

for j in data['result'].keys():
    for i in data['result'][j]['addresses']:
        out_file.write('{0},{1},{2},{3},{4},{5},{6},{7}\n'.format(j, data['result'][j]['name'], data['result'][j]['url'], data['result'][j]['coin'], data['result'][j]['category'] if data['result'][j]['category'] != 'Scam' else 'Scamming', data['result'][j]['subcategory'] if 'subcategory' in data['result'][j] else "", i, data['result'][j]['status']))

out_file.close()
