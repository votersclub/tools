#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import json
import glob
import logging
import re
import pandas
import numpy as np
from collections import defaultdict
from collections import OrderedDict

data_path = './'
n = 0

class data_passport:
    def __init__(self):
        self.n_conf_files = 0
        self.n_cf_formats = {}
        self.n_data_files = 0
        self.n_empty_conf_files = 0
        self.n_ecf_formats = defaultdict(int)
        self.n_uiks = 0
        self.n_subjects = 0
        self.n_valid_bulletins = 0
        self.n_not_valid_bulletins = 0
        self.n_given_bulletins = 0
        self.n_registered_voters = 0
        self.n_candidates = 0
        self.n_data_errors = 0
        self.data_errors = {}

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    dp = data_passport()

    logging.debug('args:')
    for a in sys.argv:
        logging.debug(a)

    args = sys.argv[1:]

    if '--data' in args:
        data_path = args[1]

    logging.info('Data directory: %s', data_path)
    formats = defaultdict(int)
    dig_formats = defaultdict(int)

    for e_cfg_file in glob.glob(os.path.join(data_path, '*.json')):
        with open(e_cfg_file) as json_data:
            ecfg = json.load(json_data)
        dp.n_conf_files += 1
        pattern = re.compile(r'(\dNL).(json)')
        res = re.search(pattern, e_cfg_file)
        if res is None:
            pattern = re.compile(r'(_\d\d).(json)')
            res = re.search(pattern, e_cfg_file)
        if res is None:
            pattern = re.compile(r'(_\d).(json)')
            res = re.search(pattern, e_cfg_file)
        if res is None:
            print e_cfg_file + ' did not match any pattern, exiting'
            exit(1)
        res = res.group(1)
        dd = re.search(r'\d+', res).group()

        if 'NL' in res:
            dp.n_empty_conf_files += 1
            dp.n_ecf_formats[dd] += 1
        else:
            if dd not in dp.n_cf_formats:
                dp.n_cf_formats[dd] = {
                    'nfiles' : 0,
                    'nuiks': 0,
                    'nsubjects': 0,
                    'nvalid_bulletins': 0,
                    'nnotvalid_bulletins': 0,
                    'ngiven_bulletins': 0,
                    'nregistered_voters': 0,
                    'ncandidates': 0,
                    'ndata_errors': 0
                }

            df = pandas.read_csv(data_path + '/' + ecfg['source_file'], encoding="utf-8", delimiter=',')
            dp.n_data_files += 1
            dp.n_cf_formats[dd]['nfiles'] += 1
            vbc = ecfg['valid_bulletins_column']
            valid_bulletins = np.array(df[vbc], dtype=float)
            dp.n_valid_bulletins += np.int(valid_bulletins.sum())
            gbc = 'calc0'
            given_bulletins = np.array(df[gbc], dtype=float)
            dp.n_given_bulletins += np.int(given_bulletins.sum())
            dp.n_cf_formats[dd]['ngiven_bulletins'] += np.int(given_bulletins.sum())
            nvbc = ecfg['not_valid_bulletins_column']
            not_valid_bulletins = np.array(df[nvbc], dtype=float)
            dp.n_not_valid_bulletins += np.int(not_valid_bulletins.sum())
            dp.n_cf_formats[dd]['nvalid_bulletins'] += np.int(valid_bulletins.sum())
            dp.n_cf_formats[dd]['nnotvalid_bulletins'] += np.int(not_valid_bulletins.sum())
            dp.n_uiks += df[vbc].count()
            dp.n_cf_formats[dd]['nuiks'] += df[vbc].count()
            dp.n_candidates += len(ecfg['candidates_columns'])
            dp.n_cf_formats[dd]['ncandidates'] += len(ecfg['candidates_columns'])
            dp.n_data_errors += len(ecfg['data_errors'])
            dp.n_cf_formats[dd]['ndata_errors'] += len(ecfg['data_errors'])
            for e in ecfg['data_errors']:
                if e['kind'] in dp.data_errors:
                    dp.data_errors[e['kind']] += 1
                else:
                    dp.data_errors[e['kind']] = 1
#                if e['kind'] == 10:
#                    print 'In ', e_cfg_file, e['comment'].encode('utf-8')
            registered_voters = np.array(df[ecfg['registered_voters_column']], dtype=float)
            registered_voters[np.isnan(registered_voters)] = 0
            dp.n_registered_voters += int(registered_voters.sum())
            dp.n_cf_formats[dd]['nregistered_voters'] += int(registered_voters.sum()) #####




    for k in dp.__dict__:
        if type(dp.__dict__[k]) in [int, np.int64]:
            logging.info('%s: %d', k, dp.__dict__[k])
        else:
            logging.info(k)
            od = OrderedDict(sorted(dp.__dict__[k].items()))
            for v in od:
                if type(od[v]) != dict:
                    logging.info('\t%s: %d', v, od[v])
                else:
                    logging.info('\t%s', v)
                    for vv in od[v]:
                        logging.info('\t\t%s: %d', vv, od[v][vv])
