#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
import os
import json
import glob

data_path = './'
n = 0

def anglify(e):
	m = hashlib.md5()
	print e['source_file'].encode('utf8')
	m.update(e['source_file'].encode('utf8'))
	en = e['year'] + e['level'] + '_' + e['type'] + '_' + m.hexdigest() + e['source_file'][-7:-4] + '.csv'
	print en
	return en

for e_cfg_file in glob.glob(os.path.join(data_path, '*.json')):
	print str(n) + ' ' + e_cfg_file
	with open(e_cfg_file) as json_data:
		ecfg = json.load(json_data)

	if ecfg['source_file'] == 'NULL':
		continue

	fname = anglify(ecfg)
	#old_name = os.path.join(data_path, ecfg ['source_file']).replace('(', '\(').replace(')', '\)')
	old_name = os.path.join(data_path, ecfg ['source_file'])
	new_name = os.path.join(data_path, fname)
	#print cmd
	if not os.path.exists(old_name):
		print old_name + ' does not exist, exiting'
	os.rename(old_name, new_name)
	ecfg ['source_file'] = fname 
	with open(e_cfg_file, 'wb') as fp:
		fp.write(json.dumps(ecfg, ensure_ascii=False, indent=4).encode("utf-8"))
		fp.close()
	n += 1
