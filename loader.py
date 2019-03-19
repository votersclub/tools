#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cProfile
import functools
import pandas as pd
import re
import json
import hashlib

from listparser import HTMLListParser
from tableparser import HTMLResultsParser
from tableparser import LoadFailedDoNotRetry
from tableparser import LoadRetryWithDifferentFormat
from tableparser import LoadFailedDifferentCandidates
from tableparser import LoadErrorNoDataMarker
from tableparser import LoadFailedEmptyCells


from tableparser import check_numbers
from tableparser import KIND_TIK_UIK_STATS_ERROR
from tableparser import KIND_TIK_UIK_CANDIDATES_STATS_ERROR
from tableparser import KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES_GLOBAL
from tableparser import KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES_IN_DATA
from tableparser import KIND_MISSING_UPPER_FIELD
from tableparser import KIND_MISSING_LOWER_FIELD
from tableparser import KIND_BADLY_FORMATTED_PAGE
from tableparser import KIND_EMPTY_CELLS
from tableparser import print_parser_stats

from columns_titles import registered_voters_column_titles
from columns_titles import valid_bulletins_column_titles
from columns_titles import not_valid_bulletins_column_titles
from columns_titles import votes_by_otkrepitelnie_column_titles


from tableparser import make_link
import os.path
import datetime
import sys
import numpy as np

source_of_data = 'http://votersclub.org'

DATA_ABSENCE_FORMAT_NOT_FOUND = 0
DATA_ABSENCE_ELECTIONS_NOT_SUPPORTED = 1
DATA_ABSENCE_NO_RESULTS_URL = 2
DATA_ABSENCE_DIFFERENT_CANDIDATES = 3
DATA_ABSENCE_NO_DATA_MARKER = 4
DATA_ABSENCE_EMPTY_CELLS = 5
MAX_DATA_ABSENCE_REASON = DATA_ABSENCE_EMPTY_CELLS

DATA_ABSENCE_TITLES = [u'Неполные данные или неподдерживаемый формат',
                       u'Формат выборов пока не поддерживается',
                       u'Нет ссылки на данные от УИК',
                       u'Выборы с несколькими одномандатными округами пока не поддерживаются',
                       u'Некоторые страницы не содержат данных',
                       u'Некоторые таблицы не содержат данных']

MAX_BAD_PAGE_ERRORS = 3

dbg_mode = False
print_level = 4

def lprint(level, msg):
    if level <= print_level:
        print '\t' * level + msg

def formats(up_level):
    if up_level == 'subject':
        if dbg_mode:
            return [10, 11, 12, 13, 14, 15, 16, 17, 18, 30]
        else:
            return [10, 11, 12, 13, 14, 15, 16, 17, 18, 30]
    elif up_level == 'federal':
        if dbg_mode:
            return [5]
        else:
            return [0, 1, 2, 3, 4, 5]
    else:
        print 'ERROR: Unknown elections level, exiting. Level: ' + up_level
        exit(1)


def profiler_decorator(func):
    @functools.wraps(func)
    def profiler_decorated_function(*args, **kwargs):
        profiler = None
        profile_filename = '/home/elections/profile'

        if profile_filename:
            print 'Profiler is enabled'
            profiler = cProfile.Profile()
            profiler.enable()

        result = func(*args, **kwargs)

        if profile_filename:
            profiler.disable()
            profiler.dump_stats(profile_filename)
            print 'Profiler stats is written to file %s', profile_filename

        return result

    return profiler_decorated_function


def add_uik_results(erecords, subject_title, uik_results):
    added_recs = 0
    tik_title = uik_results['config']['commission']['title']

    for uik in uik_results['subjects']:
        if subject_title == '' or uik['title'] == '':
            print 'ERRORERROR: empty title of subject or uik, url:'
            print uik_results['config']['commission']['href']
            exit(1)
        if tik_title == '':
            print 'ERRORERROR: empty title of tik, url:'
            tik_title = subject_title
        rec = [subject_title, tik_title, uik['title']]
        c = 0
        while c < len(uik):
            id = str(c)
            # not candidate result, just up level stat
            if id in uik_results['config']['data_columns']:
                rec.append(uik[id])
            c += 1
        # If number of data fields in uik records is less than number of fields
        # in upper config, fill in missing fields with N/A. Error about different
        # list of fields must be registered in check_elections
        if len(rec) < len (erecords[0]) - len(uik_results['config']['candidates_columns']):
            print 'ERRORERROR: add_uik_results:  number of fields in uik is smaller than in upper config'
            n_uik_data_columns = len(rec)
            n_upper_data_columns = len(erecords[0]) - len(uik_results['config']['candidates_columns'])
            diff = n_upper_data_columns - n_uik_data_columns
            i = 0
            while i < diff:
                rec.append('N/A')
                i += 1

        c = 0
        while c < len(uik):
            c_id = 'c' + str(c)
            # candidate result - there is number of votes and percent
            # skip percentage for now
            if c_id in uik_results['config']['candidates_columns']:
                rec.append(uik[c_id])
            c += 1

        erecords.append(rec)
        added_recs += 1
    if added_recs == 0:
        print 'ERRORERROR: add_uik_results: added 0 records, very unlikely event! Title: ' + subject_title
        # if there were no records added then it's highly likely we use wrong format
        # if there are data however - then we parsed all levels up to uiks at least once
        # which means it's just buggy data
        if len(erecords) == 0:
            raise LoadRetryWithDifferentFormat
    return


def add_lower_stats(upper, lower):
    for k in upper:
        if k not in lower:
            # skip rows missing in lower commission. Error should be registered in
            # check_elections (difference in the list of fields between commissions
            # of different levels
            continue
        #upper[k] = str(int(upper[k]) + int(lower[k]))
        upper[k] += lower[k]
    return

def fill_registered_voters_and_bulletins(elections):
    for id in elections['config']['columns_map']:
        title = elections['config']['columns_map'][id].lower().strip(' .')

        if title in registered_voters_column_titles:
            if 'registered_voters_column' in elections['config']:
                print 'ERRORERROR! registered_voters_column already has value: ' + elections['config']['registered_voters_column'] + \
                      ' while it must be empty'
                raise LoadFailedDoNotRetry(elections['config']['commission']['href'])
            else:
                elections['config']['registered_voters_column'] = id

        if title in valid_bulletins_column_titles:
            if 'valid_bulletins_column' in elections['config']:
                print 'ERRORERROR! valid_bulletins_column already has value: ' + elections['config']['valid_bulletins_column'] + \
                      ' while it must be empty'
                raise LoadFailedDoNotRetry(elections['config']['commission']['href'])
            else:
                elections['config']['valid_bulletins_column'] = id

        if title in not_valid_bulletins_column_titles:
            if 'not_valid_bulletins_column' in elections['config']:
                print 'ERRORERROR! not_valid_bulletins_column already has value: ' + elections['config']['not_valid_bulletin_columns'] + \
                      ' while it must be empty'
                raise LoadFailedDoNotRetry(elections['config']['commission']['href'])
            else:
                elections['config']['not_valid_bulletins_column'] = id

        if title in votes_by_otkrepitelnie_column_titles:
            if 'votes_by_otkrepitelnie_column' in elections['config']:
                print 'ERRORERROR! votes_by_otkrepitelnie_column already has value: ' + elections['config'][
                    'votes_by_otkrepitelnie_column'] + \
                      ' while it must be empty'
                raise LoadFailedDoNotRetry(elections['config']['commission']['href'])
            else:
                elections['config']['votes_by_otkrepitelnie_column'] = id

    if 'registered_voters_column' not in elections['config'] or \
            elections['config']['registered_voters_column'] not in elections['config']['columns_map']:
        print 'WARNING: registered_voters_column is not configured!'
        print 'columns_map:'
        print repr(elections['config']['columns_map']).decode('unicode-escape')
        print elections['config']['commission']['href']
        raise LoadFailedEmptyCells(elections['config']['commission']['href'])

    if 'valid_bulletins_column' not in elections['config'] or \
            elections['config']['valid_bulletins_column'] not in elections['config']['columns_map']:
        print 'WARNING: valid_bulletins_column is not configured!'
        print repr(elections['config']['columns_map']).decode('unicode-escape')
        raise LoadFailedEmptyCells(elections['config']['commission']['href'])

    if 'not_valid_bulletins_column' not in elections['config'] or \
            elections['config']['not_valid_bulletins_column'] not in elections['config']['columns_map']:
        print 'WARNING: not_valid_bulletins_column is not configured!'
        print repr(elections['config']['columns_map']).decode('unicode-escape')
        raise LoadFailedEmptyCells(elections['config']['commission']['href'])

    if 'votes_by_otkrepitelnie_column' not in elections['config'] or \
            elections['config']['votes_by_otkrepitelnie_column'] not in elections['config']['columns_map']:
        print 'WARNING: votes_by_otkrepitelnie_column is not configured!'
        print repr(elections['config']['columns_map']).decode('unicode-escape')
        if elections['config']['level'] == 'subject':
            elections['config']['votes_by_otkrepitelnie_column'] = 'NA'
        elif elections['config']['level'] == 'federal':
            elections['config']['votes_by_otkrepitelnie_column'] = 'NA'

def check_uiks_results(se, uiks_stats, uiks_candidates_stats, subj, elections):
    # Some elections results lead to UIKs data w/o stats on OIKs level, there are just links to UIKs
    if 'upper_level_stats' in se['config']:
        for k in se['config']['upper_level_stats']:
            if k in uiks_stats:
                if uiks_stats[k] != se['config']['upper_level_stats'][k]:
                    rec = {
                        'kind': KIND_TIK_UIK_STATS_ERROR,
                        'subject': subj['title'],
                        'upper_link': make_link(subj['href'], subj['title']),
                        'subj_link': '',
                        'field': k,
                        'upper_value': se['config']['upper_level_stats'][k],
                        'lower_value': uiks_stats[k],
                        'comment': u'Сумма значений от комиссии нижнего уровня отличается от сводного значения комиссии верхнего уровня'
                    }
                    elections['config']['data_errors'].append(rec)
                    print 'Data error: ' + repr(rec).decode('unicode-escape')
            else:
                # If the field is not there in uiks_stats check that at least one missing_data error is registered
                found = False
                for e in elections['config']['data_errors']:
                    if e['kind'] in [KIND_MISSING_UPPER_FIELD, KIND_MISSING_LOWER_FIELD]:
                        found = True
                        break
                if not found:
                    print 'ERRORERROR: Missing field ' + k + ' and error is not registered, exiting'
                    exit(1)
    if 'upper_level_candidates_stats' in se['config']:
        for k in se['config']['upper_level_candidates_stats']:
            if k not in uiks_candidates_stats:
                pass
            if uiks_candidates_stats[k] != se['config']['upper_level_candidates_stats'][k]:
                rec = {
                    'kind': KIND_TIK_UIK_CANDIDATES_STATS_ERROR,
                    'subject': subj['title'],
                    'upper_link': make_link(subj['href'], subj['title']),
                    'subj_link': '',
                    'field': k,
                    'upper_value': se['config']['upper_level_candidates_stats'][k],
                    'lower_value': uiks_candidates_stats[k],
                    'comment': u'Значения по кандидатам от комиссии нижнего уровня отличаются от данных комиссии верхнего уровня'
                }
                elections['config']['data_errors'].append(rec)
                print 'Data error: ' + repr(rec).decode('unicode-escape')
    return


def fill_upper_level_stats(elections, se, erecords):
    # erecords w/o header means that upper level stats were not initiated
    if len(erecords) == 0 and 'data_columns' in se['config']:
        for k in se['config']:
            if k not in elections['config']:
                elections['config'][k] = se['config'][k]

        fill_registered_voters_and_bulletins(elections)
        print "Valid bulletins:" + str(
            elections['config']['upper_level_stats'][elections['config']['valid_bulletins_column']])
        # Columns headers as the first record in bi-dimensional array
        rec = ['subject', 'tik', 'uik']

        for s in elections['config']['data_columns']:
            rec.append(s)
        erecords.append(rec)
    return


def check_that_candidates_are_same(elections, se, subj):
    if len(elections['config']['candidates_columns']) != len(se['config']['candidates_columns']):
        print 'WARNING: Different number of candidates in one elections is not yet supported'
        print 'WARNING: Different number of candidates up config: ' + str(
            len(elections['config']['candidates_columns'])) + \
              ' number of candidates in subj config: ' + str(len(se['config']['candidates_columns'])) + \
              ' subj: ' + subj['title']
        print 'subj href: ' + subj['href']
        raise LoadFailedDifferentCandidates

    for c in elections['config']['candidates_columns']:
        if elections['config']['columns_map'][c] != se['config']['columns_map'][c]:
            print 'WARNING: Different candidates in one elections are not yet supported'
            print 'WARNING: Candidates up config: ' + elections['config']['columns_map'][c] + \
                  ' subj config: ' + se['config']['columns_map'][c] + \
                  ' subj: ' + subj['title']
            print 'subj href: ' + subj['href']
            raise LoadFailedDifferentCandidates
    return


def check_elections_correctness(elections, erecords):
    data_errors = []
    config = elections['config']

    s = 0
    for c in config['candidates_columns']:
        s += config['upper_level_candidates_stats'][c]
    if s != config['upper_level_stats'][config['valid_bulletins_column']]:
        rec = {
            'kind': KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES_GLOBAL,
            'subject': elections['config']['commission']['title'],
            'upper_link': make_link(elections['config']['commission']['href'],
                                    elections['config']['commission']['title']),
            'subj_link': '',
            'field': config['valid_bulletins_column'],
            'upper_value': config['upper_level_stats'][config['valid_bulletins_column']],
            'lower_value': s,
            'comment': u'Сумма голосов за кандидатов не равна количеству действительных бюллетеней'
        }
        data_errors.append(rec)
    config = elections['config']
    vbc = config['valid_bulletins_column']
    df = pd.DataFrame(erecords[1:], columns=erecords[0])
    valid_bulletins = np.array(df[vbc], dtype=float)
    valid_bulletins[np.isnan(valid_bulletins)] = 0
    valid_bulletins_sum = valid_bulletins.sum()

    votes_sum = 0
    for c in config['candidates_columns']:
        votes = np.array(df[c], dtype=float)
        votes[np.isnan(votes)] = 0
        votes_sum += votes.sum()

    if votes_sum != valid_bulletins_sum:
        rec = {
            'kind': KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES_IN_DATA,
            'subject': elections['config']['commission']['title'],
            'upper_link': make_link(elections['config']['commission']['href'],
                                    elections['config']['commission']['title']),
            'subj_link': '',
            'field': config['valid_bulletins_column'],
            'upper_value': config['upper_level_stats'][config['valid_bulletins_column']],
            'lower_value': s,
            'comment': u'Сумма голосов за кандидатов не равна количеству действительных бюллетеней'
        }
        data_errors.append(rec)
        print 'Data error: ' + repr(rec).decode('unicode-escape')

    return data_errors


def process_uik_page(fmt, page_level, subj_elections, top_elections,
                     erecords, subj, uiks_stats, uiks_candidates_stats, gs, subj_title):
    try:
        #if 'commission' in subj['config']:
        #    subj_title = subj['config']['commission']['title']
        #else:
        #    subj_title = subj['title']
        parser = HTMLResultsParser(subj_elections['config']['uiks_results'], fmt, True, page_level)
        uik_results = parser.parse_results_page(top_elections['config'], gs)
        add_uik_results(erecords, subj_title, uik_results)
        add_lower_stats(uiks_stats, uik_results['config']['upper_level_stats'])
        add_lower_stats(uiks_candidates_stats, uik_results['config']['upper_level_candidates_stats'])
        check_uiks_results(subj_elections, uiks_stats, uiks_candidates_stats, subj_elections, top_elections)
        if 'tik_data' in uik_results['config']:
            top_elections['config']['tik_data'].append(uik_results['config']['tik_data'])
            print 'FOUND TIK DATA AT ' + subj_elections['config']['uiks_results']

    except LoadFailedEmptyCells as e:
        # If there are data added already it rather means empty UIK page, not necessary to change the format
        if len(erecords) > 1:
            rec = {
                'kind': KIND_EMPTY_CELLS,
                'subject': subj_title,
                'upper_link': make_link(subj_elections['config']['uiks_results'], subj_title),
                'subj_link': '',
                'field': '',
                'upper_value': '',
                'lower_value': '',
                'comment': u'Таблица содержит пустые ячейки'
            }
            top_elections['config']['data_errors'].append(rec)
        else:
            raise LoadFailedEmptyCells(e.url)

    return


def check_page_level(page_level, fmt):
    if fmt in [14, 5, 12]:
        max_level = 4
    elif fmt == 15:
        max_level = 5
    else:
        max_level = 3
    if page_level > max_level:
        print 'ERRORERROR, page_level is too big: ' + str(page_level)
        exit(1)
    return


def load_lower_levels(upper_elections, elections, fmt, erecords, page_level, gs, up_subj_title):
    check_page_level(page_level, fmt)
    indent = page_level * '\t'

    lprint(page_level, 'Subjects: ' + str(len(elections['subjects'])))

    filled_up_level_stats = False

    for subj in elections['subjects']:
        if up_subj_title == '':
            subj_title = subj['title']
        else:
            subj_title = up_subj_title

        lprint(page_level, subj['title'] + ', ' + str(subj['id']) +
               ' out of ' + str(len(elections['subjects'])))

        # Save link to the page which contains upper level stats for the subject
        subj['upper_href'] = upper_elections['config']['results_href']
        subj['upper_title'] = upper_elections['config']['href_title']

        if 'is_uiks_page' in subj:
            uiks_stats, uiks_candidates_stats = init_uiks_stats(upper_elections)
            if 'config' not in subj:
                subj['config'] = {}
            subj['config']['uiks_results'] = subj['href']
            process_uik_page(fmt, page_level + 1, subj, upper_elections, erecords, subj,
                             uiks_stats, uiks_candidates_stats, gs, subj_title)
        else:
            parser = HTMLResultsParser(subj['href'], fmt, False, page_level)
            try:
                se = parser.parse_results_page(upper_elections['config'], gs)
                if 'tik_data' in se['config']:
                    upper_elections['config']['tik_data'].append(se['config']['tik_data'])
                    print 'FOUND TIK DATA AT ' + subj['href']
            # Check if we already loaded anything with this format. If we did, it's possible
            # That format is selected correctly and it's just broken page. It happens.
            # If it's not the first broken page though, let's reraise exception
            except LoadRetryWithDifferentFormat:
                # If there are records in erecords besides header then we already processed something
                # down to UIK level included. There is a chance it's just a broken page. In this case just skip
                # this subject
                if (len(erecords)) > 1:
                    nerrors = 0
                    for er in upper_elections['config']['data_errors']:
                        if er['kind'] == KIND_BADLY_FORMATTED_PAGE:
                            nerrors += 1
                    if nerrors >= MAX_BAD_PAGE_ERRORS:
                        raise
                    if up_subj_title != '':
                        stitle = up_subj_title + ' - ' + subj['title']
                    else:
                        stitle = subj['title']
                    rec = {
                        'kind': KIND_BADLY_FORMATTED_PAGE,
                        'subject': subj['title'],
                        'upper_link': '',
                        'subj_link': make_link(subj['href'], stitle),
                        'field': '',
                        'upper_value': '',
                        'lower_value': '',
                        'comment': 'Формат страницы неизвестен'
                    }
                    upper_elections['config']['data_errors'].append(rec)
                    print "WARNING: KIND_BADLY_FORMATTED_PAGE, added data_error and proceeed"
                    print "url: " + subj['href']
                    continue
                else:
                    raise
            se['config']['results_link'] = make_link(subj['href'], subj['title'])

            # Up level stats in some formats are all empty, need to be copied from page of lower level
            if 'columns_map' not in upper_elections['config'] and 'columns_map' in se['config'] and \
                    not filled_up_level_stats:
                    fill_upper_level_stats(upper_elections, se, erecords)
                    filled_up_level_stats = True

            if 'columns_map' in upper_elections['config'] and 'columns_map' in se['config']:
                check_that_candidates_are_same(upper_elections, se, subj)

            # Some pages are empty with just links to next level in fmt 4
            if page_level == 1 and 'columns_map' in se['config']:
                de = check_numbers(subj, se['config'])
                for rec in de:
                    upper_elections['config']['data_errors'].append(rec)

            if parser.page_with_link_to_uiks:
                uiks_stats, uiks_candidates_stats = init_uiks_stats(upper_elections)
                process_uik_page(fmt, page_level + 1, se, upper_elections,
                                 erecords, elections, uiks_stats, uiks_candidates_stats, gs, subj_title)
                #check_uiks_results(se, uiks_stats, uiks_candidates_stats, subj, upper_elections)
            else:
                load_lower_levels(upper_elections, se, fmt, erecords, page_level + 1, gs, subj_title)

            subj['elections'] = se

    if page_level == 1:
        derr = check_elections_correctness(elections, erecords)
        for rec in derr:
            elections['config']['data_errors'].append(rec)

        lprint(page_level, 'Loading complete ...')

    return


def init_uiks_stats(elections):
    if 'upper_level_stats' not in elections['config']:
        pass
    uiks_stats = dict([(k, 0) for k in elections['config']['upper_level_stats']])
    uiks_candidates_stats = dict([(k, 0) for k in elections['config']['upper_level_candidates_stats']])
    return uiks_stats, uiks_candidates_stats


def add_elections_params(elections, number, up_level, date, typ, location, participants,
                          results_href, generic_href, csv_file_name=None, fmt=None):
    if 'config' not in elections:
        elections['config'] = {}

    elections['config']['number'] = number
    elections['config']['level'] = up_level
    elections['config']['date'] = date
    elections['config']['year'] = date.split('.')[2]
    elections['config']['type'] = typ
    elections['config']['location'] = location
    elections['config']['results_link'] = make_link(results_href, u"ЦИК РФ")
    elections['config']['results_href'] = results_href
    elections['config']['generic_href'] = generic_href
    elections['config']['href_title'] = u'ЦИК РФ'
    elections['config']['source_file'] = csv_file_name
    elections['config']['participants'] = participants
    elections['config']['commission_column'] = 'subject'
    elections['config']['format'] = fmt
    elections['config']['as_of_date'] = str(datetime.datetime.now().date().strftime('%d.%m.%Y'))
    elections['config']['as_of_time'] = str(datetime.datetime.now().time())
    elections['config']['source_of_data'] = source_of_data
    if 'tik_data' not in elections['config']:
        elections['config']['tik_data'] = []
    else:
        rec = elections['config']['tik_data']
        elections['config']['tik_data'] = []
        elections['config']['tik_data'].append(rec)
        print 'FOUND TIK DATA AT TOP PAGE, ADDED'

    return elections


def load_elections_fmt(number, fmt, up_level, date, typ, location,
                       participants, target_dir, fname_base, results_href, generic_href):

    csv_file_name = fname_base + '.csv'
    top_parser = HTMLResultsParser(results_href, fmt, False, 0)
    gs = 0
    elections = top_parser.parse_results_page(None, gs)
    elections = add_elections_params(elections, number, up_level, date, typ, location, participants,
                                     results_href, generic_href, csv_file_name=csv_file_name, fmt=fmt)
    erecords = []
    fill_upper_level_stats(elections, elections, erecords)

    load_lower_levels(elections, elections, fmt, erecords, 1, gs, '')
    print 'Global sum: ' + str(gs)
    print 'Loaded tik data of length ' + str(len(elections['config']['tik_data']))
    if len(elections['config']['tik_data']) == 0:
        pass
    else:
        print 'Subjects at 0 item: ' + str(len(elections['config']['tik_data'][0]['subjects']))

    df = pd.DataFrame(erecords)
    if df.shape[0] == 0:
        print 'WARNING: Empty erecords'
        raise LoadRetryWithDifferentFormat
    elif df.shape[0] == 1:
        print 'WARNING: There is only header in erecords'
        raise LoadRetryWithDifferentFormat

    with open(target_dir + '/' + fname_base + '.json', 'wb') as fp:
        fp.write(json.dumps(elections['config'], ensure_ascii=False, indent=4).encode("utf-8"))
        fp.close()

    df.to_csv(target_dir + '/' + csv_file_name, encoding="utf-8", mode='w', sep=',', index=False, header=False)
    print 'Done'
    return


def save_empty_config(number, dar, dau, up_level, date, typ, location,
                      participants, target_dir, fname_base, results_href, title, generic_href):
    elections = {'config': {}}
    elections = add_elections_params(elections, number, up_level, date, typ, location,
                                     participants, results_href, generic_href, csv_file_name='NULL', fmt='NULL')
    elections['config']['data_absence_reason'] = dar
    elections['config']['data_absence_comment'] = DATA_ABSENCE_TITLES[dar]
    if dau != '':
        elections['config']['data_absence_url'] = dau
    elections['config']['el_title'] = title

    with open(target_dir + '/' + fname_base + '.json', 'wb') as fp:
        fp.write(json.dumps(elections['config'], ensure_ascii=False, indent=4).encode("utf-8"))

    return


def files_there(formats, target_dir, target_file_name_base):
    found = False

    for fmt in formats:
        if not found:
            fullnamecsv=target_dir + '/' + target_file_name_base + '_' + str(fmt)+ '.csv'
            fullnamejson=target_dir + '/' + target_file_name_base + '_' + str(fmt) + '.json'
            f1 = os.path.isfile(fullnamecsv)
            f2 = os.path.isfile(fullnamejson)
            found = f1 and f2
    if not found:
        for i in range(MAX_DATA_ABSENCE_REASON + 1):
            found = (os.path.isfile(target_dir + '/' + target_file_name_base + str(i) + 'NL.json'))
            if found:
                break
    if not found:
        for i in range(MAX_DATA_ABSENCE_REASON + 1):
            found = (os.path.isfile(target_dir + '/' + target_file_name_base + str(i) + 'NLc.json'))
            if found:
                break
    return found


def load_elections(up_level, number, date, type, location, participants,
                   target_dir, target_file_name_base, results_href, title, generic_href):
    fmts = formats(up_level)
    fmt = min(fmts)

    if files_there(fmts, target_dir, target_file_name_base):
        print 'File is already there'
        return True

    data_absence_reason = -1
    data_absence_url = ''
    while fmt <= max(fmts) and results_href != 'NULL':
        if fmt in fmts:
            fname_base = target_file_name_base + '_' + str(fmt)
            try:
                print 'Trying format ' + str(fmt)
                load_elections_fmt(number, fmt, up_level, date, type, location, participants,
                                   target_dir, fname_base, results_href, generic_href)
                print 'Elections processed, target_file_name_base:' + fname_base
                return True
            except LoadRetryWithDifferentFormat:
                print 'WARNING! Exception LoadRetryWithDifferentFormat, tried format ' + str(fmt)
                fmt += 1
            except LoadFailedDoNotRetry as e:
                print 'WARNING! Exception LoadFailedDoNotRetry'
                data_absence_reason = DATA_ABSENCE_ELECTIONS_NOT_SUPPORTED
                data_absence_url = e.url
                break
            except LoadFailedDifferentCandidates:
                print 'WARNING! Exception LoadFailedDifferentCandidates'
                data_absence_reason = DATA_ABSENCE_DIFFERENT_CANDIDATES
                break
            except LoadErrorNoDataMarker as e:
                print 'WARNING! Exception LoadErrorNoDataMarker'
                data_absence_reason = DATA_ABSENCE_NO_DATA_MARKER
                data_absence_url = e.url
                break
            except LoadFailedEmptyCells as e:
                print 'WARNING! Exception LoadFailedEmptyCells'
                data_absence_reason = DATA_ABSENCE_EMPTY_CELLS
                data_absence_url = e.url
                break
        else:
            fmt += 1

    if results_href == 'NULL':
        data_absence_reason = DATA_ABSENCE_NO_RESULTS_URL
    elif data_absence_reason == -1:
        data_absence_reason = DATA_ABSENCE_FORMAT_NOT_FOUND
    fname_base = target_file_name_base + str(data_absence_reason) + 'NL'
    # We only can get here is did not manage to load or parse elections results. In this case
    # save empty config to make it visible in dashboard
    print 'WARNING! Did not manage to parse elections from url ' + results_href
    print 'Saving as empty config with name ' + fname_base
    save_empty_config(number, data_absence_reason, data_absence_url, up_level, date, type, location, participants, target_dir,
                      fname_base, results_href, title, generic_href)
    print_parser_stats()

    return False


months = {u'января':'01', u'февраля': '02', u'марта': '03', u'апреля': '04', u'мая': '05', u'июня': '06',
             u'июля': '07', u'августа': '08', u'сентября': '09', u'октября':'10', u'ноября': '11', u'декабря': '12'}


def load_elections_list(sourcefile, target_dir):

    parser = HTMLListParser()
    elections_list = parser.parse_elections_list_file(sourcefile)
    number = 0
    for e in elections_list['elections']:
        e['number'] = number
        number += 1
        #set date
        s = e['date'].split(' ')
        dt = s[0] + '.' + months[s[1]] + '.' + s[2]
        e['date'] = dt

        #set year
        e['year'] = s[2]

        #set type (person/parliament)
        if (re.search(u'губернатора ', e['title'].lower()) is not None) or \
            (re.search(u'мэра ', e['title'].lower()) is not None):
            e['type'] = 'mayor'
            e['participants'] = 'people'
        elif re.search(u'депутата ', e['title'].lower()) is not None:
            e['type'] = 'deputy'
            e['participants'] = 'people'
        elif (re.search(u'президента ', e['title'].lower()) is not None) or \
            (re.search(u'главы ', e['title'].lower()) is not None):
            e['type'] = 'president'
            print 'assigned "president" type to elections with title ' + e['title']
        elif re.search(u'референдум ', e['title'].lower()) is not None:
            e['type'] = 'referendum'
            print 'assigned "referendum" type to elections with title ' + e['title']
        else:
            e['type'] = 'parliament'
            e['participants'] = 'parties'
            print 'assigned "parliament" type to elections with title ' + e['title']
    fname = target_dir + '/' + elections_list['level'] + '_elections_list.jsn'
    with open(fname, 'wb') as fp:
        fp.write(json.dumps(elections_list, ensure_ascii=False, indent=4).encode("utf-8"))
        fp.close()
    print 'After saving json'
    return


def short_name(s, ml):
    l = len(s.encode('utf8'))
    m = hashlib.md5()
    m.update(s.encode('utf8'))
    hl = len(m.hexdigest())
    while l >= ml - hl:
        s = s[:-1]
        l = len(s.encode('utf8'))
    s = s + m.hexdigest()
    return s


def build_name_base(level, e):
    # Max len of file name on Ubuntu is 255, save 10 bytes for format number and just in case
    max_len = 242
    location =  e['location'].replace(' ', '_').replace('\n','_')
    title = e['title'].replace(' ', '').replace('\n', '')

    file_name_base = e['year'] + level + '_' + e['type'] + '_' + \
        location + '_' + e['date'] + '_' +  title + str(e['number'])

    l = len(file_name_base.encode('utf8'))
    if l > max_len:
        file_name_base = short_name(file_name_base, max_len)

    return file_name_base


def load_elections_from_list(level, list_file_name, target_dir):

    with open(list_file_name) as json_data:
        elections_list = json.load(json_data)

    i = 0
    nloaded = 0
    total_elections = len(elections_list['elections'])
    nreferendums = 0
    n_empty_results = 0

    for e in elections_list['elections']:

        target_file_name_base = build_name_base(elections_list['level'], e)

        print 'Elections number ' + str(i) + ' (out of ' + str(total_elections) + \
              ') target_file_name_base: ' + target_file_name_base
        print e['results_href']
        if e['type'] != 'referendum':
            if load_elections(elections_list['level'], e['number'], e['date'], e['type'], e['location'], e['type'], target_dir,
                                           target_file_name_base, e['results_href'], e['title'], e['generic_href']):
                nloaded += 1
            else:
                n_empty_results += 1
        else:
            nreferendums += 1

        print '\n\n'
        i += 1

    print 'Loading of ' + list_file_name + ' done, nloaded: ' + \
          str(nloaded) + ' referendums:' + str(nreferendums) + ' empty results_links: ' + str(n_empty_results)
    print 'Total: ' + str(nloaded + nreferendums + n_empty_results) + ' out of ' + str(total_elections)
    print '************************************************************'
    return


if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')

    targetdir = 'data'
    for a in sys.argv:
        print a
    args = sys.argv[1:]
    print "Target directory: " + targetdir
    print "Working directory: " + os.getcwd()
    if 'subject' in args:
        load_elections_list(targetdir + '/region_list.htm', targetdir)
        load_elections_from_list('subject', targetdir + '/subject_elections_list.jsn', targetdir)
    elif 'federal' in args:
        load_elections_list(targetdir + '/federal_list.htm', targetdir)
        load_elections_from_list('federal', targetdir + '/federal_elections_list.jsn', targetdir)

    print_parser_stats()
    exit(0)
