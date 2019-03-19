#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from time import sleep
from urlparse import urlparse
from urlparse import urljoin
import re

KIND_BETWEEN_PAGES_ERROR_TIK = 0
KIND_TIK_UIK_STATS_ERROR = 1
KIND_IN_PAGE_ERROR = 2
KIND_MISSING_LOWER_FIELD = 3
KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES = 4
KIND_TIK_UIK_CANDIDATES_STATS_ERROR = 5
KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES_GLOBAL = 6
KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES_IN_DATA = 7
KIND_MISSING_UPPER_FIELD = 8
KIND_EMPTY_CELLS = 9
KIND_TURNOUT_HIGHER_THAN_100 = 10
KIND_BADLY_FORMATTED_PAGE = 11
KIND_ERROR_GROUP = 999
KIND_COLUMNS_HEADERS = 1000

ERROR_DESCRIPTIONS = \
    [u'Данные комиссий разного уровня различаются',
        u'Сумма значений от комиссии нижнего уровня отличается от сводного значения комиссии верхнего уровня',
        u'Сумма значений от комиссий нижнего уровня отличается от данных комиссии верхнего уровня '
        u'(разные значения в левой и правой таблицах)',
        u'Поле в данных комиссии одного уровня отсутствует в данных комиссии другого уровня',
        u'Сумма голосов за кандидатов не равна количеству действительных бюллетеней',
        u'Значения по кандидатам от комиссии нижнего уровня отличаются от данных комиссии верхнего уровня',
        u'Сумма голосов за кандидатов не равна количеству действительных бюллетеней',
        u'Сумма голосов за кандидатов не равна количеству действительных бюллетеней',
        u'Поле в данных комиссии одного уровня отсутствует в данных комиссии другого уровня',
        u'Таблица содержит пустые ячейки',
        u'Явка больше 100%',
        u'Формат страницы неизвестен',
     ]
ERROR_COLUMNS = [
    [u'Верхняя комиссия',  u'Нижняя комиссия', u'Данные верхней комиссии', u'Данные нижней комиссии', u'Поле'],  #0
    [u'Верхняя комиссия', u'', u'Данные верхней комиссии', u'Данные нижней комиссии', u'Поле'], #1
    [u'Верхняя комиссия', u'', u'Данные верхней комиссии', u'Данные нижней комиссии', u'Поле'], #2
    [u'Верхняя комиссия', u'Нижняя комиссия', u'Значение', u'', u'Поле'], #3
    [u'Верхняя комиссия', u'', u'Действительных бюллетеней', u'Сумма голосов за кандидатов', u''], #4
    [u'Верхняя комиссия', u'', u'Данные верхней комиссии', u'Данные нижней комиссии', u'Кандидат'], #5
    [u'Верхняя комиссия', u'', u'Действительных бюллетеней', u'Сумма голосов за кандидатов', u''], #6
    [u'Верхняя комиссия', u'', u'Действительных бюллетеней', u'Сумма голосов за кандидатов', u''],  #7
    [u'Верхняя комиссия', u'Нижняя комиссия', u'', u'Значение', u'Поле'], #8
    [u'Комиссия',       u'', u'', u'', u''], #9
    [u'Комиссия',       u'', u'Бюллетеней (действ. + недейств.)', u'Избирателей', u''], #10
    [u'',       u'Страница', u'', u'', u'']
]

levels = {u'федеральный': 'federal',
          u'региональный': 'subject',
          u'административный центр': 'local',
          u'местное самоуправление': 'local'}

PATTERNS_LINKS_TO_RESULT_DATA = [u'Сводная таблица результатов выборов по единому многомандатному округу',
                                 u'Сводная таблица итогов голосования по пропорциональной ',
                                 u'Сводная таблица результатов выборов по единому',
                                 u'Сводная таблица предварительных итогов голосования',
                                 u'Результаты референдума',
                                 u'Сводная таблица итогов голосования по федеральному округу',
#                                 u'Сводная таблица итогов голосования по мажоритарной системе выборов(Протокол №1)',
                                 u'Сводная таблица результатов выборов',
                                 u'Сводная таблица итогов голосования',
                                 u'Сводный отчет об итогах голосования',
                                 u'Сводная таблица о результатах выборов']

HEADERS_AT_RESULTS_PAGE = [u'Сводная таблица результатов выборов',
                               u'Сводная таблица итогов голосования',
                               u'Сводная таблица предварительных итогов голосования',
                               u'Сводная таблица результатов выборов по федеральному избирательному округу',
                               u'Сводная таблица итогов голосования по федеральному избирательному округу',
                               u'Сводная таблица результатов выборов по единому округу',
                               u'Сводная таблица предварительных итогов голосования по единому округу',
                               u'Сводная таблица результатов выборов по единому многомандатному округу',
                               u'Сводная таблица итогов голосования по мажоритарной системе выборов(Протокол №1)',
                               u'Итоги голосования']

nrec = 0
no_results_href = 0


class LoadRetryWithDifferentFormat(Exception):
    pass


class LoadFailedDoNotRetry(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return repr(self.url)


class LoadFailedDifferentCandidates(Exception):
    pass


class LoadErrorNoDataMarker(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return repr(self.url)


class LoadFailedEmptyCells(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return repr(self.url)

def check_min_len(rr_cc, target_len, error_message):
    if len(rr_cc) < target_len:
        print error_message
        raise LoadRetryWithDifferentFormat

def check_len(rr_cc, target_len, error_message):
    if len(rr_cc) != target_len:
        print error_message
        raise LoadRetryWithDifferentFormat


def check_lens(rr_cc, target_lens, error_message):
    if len(rr_cc) not in target_lens:
        print error_message
        raise LoadRetryWithDifferentFormat


def check_text (r_c, target_texts, error_message):
    if not any_text_is_there(target_texts, r_c.get_text()):
        print error_message
        print r_c.get_text().strip()
        print repr(target_texts).decode('unicode-escape')
        raise LoadRetryWithDifferentFormat


def check_not_empty(s, error_message):
    if s == '' or s is None:
        print error_message
        raise LoadRetryWithDifferentFormat


def make_link (href, title):
    return "<a href=\'" + href + "\' rel=\'nofollow\' " + " target=\'_blank\'" + ">" + title + "</a>"


global_sum = 0


def check_elections(elections, page_with_link_to_uiks, page_with_uiks, up_config, page_level, gs):
    data_errors = []
    config = elections['config']
    subjects = elections['subjects']
    check_not_empty(config['commission'], 'info: commission must be not empty')
    if page_level == 0 or 'el_title' not in up_config or up_config['el_title'] is None or up_config['el_title'] == '':
        if 'el_title' not in config:
            print 'el_title not in config here: ' + elections['config']['commission']['href']
            raise LoadRetryWithDifferentFormat
        check_not_empty(config['el_title'], 'info: elections title must be not empty')
    if config['commission']['title'] == '':
        print 'ERRORERROR: commission title must be not empty, url:'
        print  elections['config']['commission']['href']

    if len(subjects) == 0:
        if page_level == 0:
            print 'info: no next level commissions to get data from! '
            raise LoadRetryWithDifferentFormat

        if not page_with_uiks and 'is_uiks_page' not in elections and 'uiks_results' not in elections['config']:
            print 'ERRORERROR: did not return subjects and not uiks page here:'
            print elections['config']['commission']['href']
            #exit(1)

    if ('upper_level_candidates_stats' in config) and up_config is not None and 'candidates_columns' in up_config and \
            (len(config['upper_level_candidates_stats']) != len(up_config['candidates_columns'])):
        print 'Number of candidates stats is not equal to number of candidates'
        print 'info: Likely this type of elections is not yet supported'
        raise LoadFailedDifferentCandidates
    # If subj only contains id, href and title, there is nothing to check about it
    skip_data_checks = False
    for subj in subjects:
        if len(subj) < 3:
            print 'ERRORERROR: subj cannot contain less than 3 fields'
            exit(1)
        elif len(subj) == 3:
            skip_data_checks = True
        elif len(subj) == 4 and 'is_uiks_page' in subj:
            skip_data_checks = True

    if len(subjects) == 0:
        skip_data_checks = True

    if 'upper_level_stats' in config and up_config is not None and 'data_columns' in up_config:
        data_errors = []
        for k in up_config['data_columns']:
            if k not in config['upper_level_stats'] and k not in config['upper_level_candidates_stats']:
                if k in up_config['upper_level_stats']:
                    val = up_config['upper_level_stats'][k]
                else:
                    val = up_config['upper_level_candidates_stats'][k]
                rec = {
                    'kind': KIND_MISSING_LOWER_FIELD,
                    'subject': up_config['commission']['title'],
                    'upper_link': up_config['results_link'],
                    'subj_link': make_link(config['commission']['href'], config['commission']['title']),
                    'field': k,
                    'upper_value': val,
                    'lower_value': '',
                    'comment': u'Поле в данных комиссии одного уровня отсутствует в данных комиссии другого уровня'
                }
                data_errors.append(rec)
                print 'data error: ' + repr(rec).decode('unicode-escape')
                # Add missing field so that length of the record correspond to the shape of erecords
                for subj in subjects:
                    # If there are data fields in subj
                    if len(subj) > 4:
                        subj[k] = 0
            else:
                # Ensure that columns titles are the same
                if config['columns_map'][k] != up_config['columns_map'][k]:
                    print "ERRORERROR: Columns differ"
                    print "k: " + k
                    print "config:" + config['columns_map'][k]
                    print "up_config:" + up_config['columns_map'][k]
        i = 0
        while i < len(config['data_columns']):
            k = config['data_columns'][i]
            if k not in up_config['upper_level_stats'] and k not in up_config['upper_level_candidates_stats']:
                if k in config['upper_level_stats']:
                    val = config['upper_level_stats'][k]
                    config['upper_level_stats'].pop(k, None)
                else:
                    val = config['upper_level_candidates_stats'][k]
                    config['upper_level_candidates_stats'].pop(k, None)
                rec = {
                    'kind': KIND_MISSING_UPPER_FIELD,
                    'subject': up_config['commission']['title'],
                    'upper_link': up_config['results_link'],
                    'subj_link': make_link(config['commission']['href'], config['commission']['title']),
                    'field': config['columns_map'][k],
                    'upper_value': '',
                    'lower_value': val,
                    'comment': u'Поле в данных комиссии одного уровня отсутствует в данных комиссии другого уровня'
                }
                data_errors.append(rec)
                print 'data error: ' + repr(rec).decode('unicode-escape')
                # Delete excessive field so that length of the record correspond to the shape of erecords
                for subj in subjects:
                    if k in subj:
                        subj.pop(k, None)

                config['data_columns'].remove(k)
            else:
                i += 1
                # Ensure that columns titles are the same
                if config['columns_map'][k] != up_config['columns_map'][k]:
                    print "ERRORERROR: Columns differ"
                    print "k: " + k
                    print "config:" + config['columns_map'][k]
                    print "up_config:" + up_config['columns_map'][k]

    # Right table with subj results is empty in tik level, do not check
    if not skip_data_checks:
        if 'upper_level_stats' in config:
            for stat in config['upper_level_stats']:
                s = 0
                for subj in subjects:
                    s += subj[stat]
                if config['upper_level_stats'][stat] != s:
                    rec = {
                        'kind': KIND_IN_PAGE_ERROR,
                        'subject': elections['config']['commission']['title'],
                        'upper_link': make_link(elections['config']['commission']['href'],
                                               elections['config']['commission']['title']),
                        'subj_link': '',
                        'field': stat,
                        'upper_value': config['upper_level_stats'][stat],
                        'lower_value': s,
                        'comment': u'Сумма значений от комиссий нижнего уровня отличается от данных комиссии верхнего уровня (разные значения в левой и правой таблицах)'
                    }
                    data_errors.append(rec)
                    print 'data error: ' + repr(rec).decode('unicode-escape')
        if up_config is not None and 'valid_bulletins_column' in up_config:
            # Check that sum of votes for candidates is equal to total number of votes in string
            for subj in subjects:
                s = 0
                for c in elections['config']['candidates_columns']:
                    if c not in subj:
                        pass
                    s += subj[c]
                if 'valid_bulletins_column' not in up_config:
                    pass
                if up_config['valid_bulletins_column'] not in subj:
                    print 'info: No row with valid_bulletins_column'
                    print 'LoadFailedEmptyCells'
                    raise LoadFailedEmptyCells(elections['config']['commission']['href'])
                if s != subj[up_config['valid_bulletins_column']]:
                    rec = {
                        'kind': KIND_CANDIDATES_SUM_NOT_EQUAL_TO_ALL_VOTES,
                        'subject': elections['config']['commission']['title'] + ' ' + subj['title'],
                        'upper_link': make_link(elections['config']['commission']['href'],
                                               elections['config']['commission']['title'] + ' ' + subj['title']),
                        'subj_link': '',
                        'field': up_config['valid_bulletins_column'],
                        'upper_value': subj[up_config['valid_bulletins_column']],
                        'lower_value': s,
                        'comment': u'Сумма голосов за кандидатов не равна количеству действительных бюллетеней'
                    }
                    data_errors.append(rec)
                    print 'data error: ' + repr(rec).decode('unicode-escape')
                if page_with_uiks:
                    gs += s

            # Check that turnout is less than 100%
            for subj in subjects:
                s = 0
                if up_config['valid_bulletins_column'] not in subj:
                    print 'info: No row with valid_bulletins_column'
                    print 'LoadFailedEmptyCells'
                    raise LoadFailedEmptyCells(elections['config']['commission']['href'])
                nb = subj[up_config['valid_bulletins_column']] + subj[up_config['not_valid_bulletins_column']]
                vv = subj[up_config['registered_voters_column']]
                if nb > vv:
                    rec = {
                        'kind': KIND_TURNOUT_HIGHER_THAN_100,
                        'subject': elections['config']['commission']['title'] + ' ' + subj['title'],
                        'upper_link': make_link(elections['config']['commission']['href'],
                                                elections['config']['commission']['title'] + ' ' + subj['title']),
                        'subj_link': '',
                        'field': '',
                        'upper_value': nb,
                        'lower_value': vv,
                        'comment': u'Явка превышает 100% '
                    }
                    data_errors.append(rec)
                    print 'data error: ' + repr(rec).decode('unicode-escape')
                if page_with_uiks:
                    gs += s
    if not page_with_link_to_uiks:
        for subj in subjects:
            if subj['title'] == '' or (not page_with_uiks and subj['href'] == ''):
                print 'Mandatory field of subj is empty'
                raise LoadFailedDoNotRetry(config['commission']['href'])
    return data_errors


def check_numbers(upper, lower):
    data_errors = []
    for k in upper:
        if k in lower['upper_level_stats']:
            if upper[k] != lower['upper_level_stats'][k]:
                rec = {
                    'kind': KIND_BETWEEN_PAGES_ERROR_TIK,
                    'subject': upper['title'],
                    'upper_link': make_link(upper['upper_href'], upper['upper_title']),
                    'subj_link': make_link(lower['commission']['href'], lower['commission']['title']),
                    'field': k,
                    'upper_value': upper[k],
                    'lower_value': lower['upper_level_stats'][k],
                    'comment': u'Данные комиссий разного уровня различаются'
                }
                data_errors.append(rec)
                print 'data error: ' + repr(rec).decode('unicode-escape')

        elif k in lower['upper_level_candidates_stats']:
            if upper[k] != lower['upper_level_candidates_stats'][k]:
                rec = {
                    'kind': KIND_BETWEEN_PAGES_ERROR_TIK,
                    'subject': upper['title'],
                    'upper_link': make_link(upper['upper_href'], upper['upper_title']),
                    'subj_link': make_link(lower['commission']['href'], lower['commission']['title']),
                    'field': k,
                    'upper_value': upper[k],
                    'lower_value': lower['upper_level_candidates_stats'][k],
                    'comment': u'Данные комиссий разного уровня различаются'
                }
                data_errors.append(rec)
                print 'data error: ' + repr(rec).decode('unicode-escape')
    return data_errors


n_pages_got = 0
n_pages_exceptions = 0
n_pages_retried = 0
n_pages_got_after_retries = 0


def print_parser_stats():
    print 'n_pages_got', n_pages_got
    print 'n_pages_exceptions', n_pages_exceptions
    print 'n_pages_retried', n_pages_retried
    print 'n_pages_got_after_retries', n_pages_got_after_retries
    return


def get_safe(link):
    global n_pages_got
    global n_pages_exceptions
    global n_pages_retried
    global n_pages_got_after_retries

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
    }
    n_attempts = 0
    done = False
    max_attempts = 1000
    while n_attempts < max_attempts and not done:
        try:
            bad_page = True
            n_retries = 0
            max_retries = 3
            while n_retries < max_retries and bad_page:
                bad_page = False
                sleep(0.1)
                response = requests.get(link, headers, timeout=5)
                if response.status_code != 200:
                    print ('get_safe: status_code: %d', response.status_code)
                    print ('get_safe: url %s', link)
                page = BeautifulSoup(response.text,'html.parser')
                for table in page.find_all('table'):
                    if not bad_page:
                        for r in table.find_all('tr'):
                            if not bad_page:
                                for c in r.find_all('td'):
                                    if not bad_page and c.get_text().strip() == u'Нет данных для построения отчета.':
                                        bad_page = True
                                        if n_retries == 0:
                                            n_pages_retried += 1
                if bad_page:
                    n_retries += 1

            if n_retries != 0 and not bad_page:
                n_pages_got_after_retries += 1
            done = True
            n_pages_got += 1
        except:
            print 'requests.get failed, attempt ' + str(n_attempts)
            sleep(3)
            n_pages_exceptions += 1
            pass
        n_attempts += 1
    if n_attempts == max_attempts:
        print 'ERRORERROR: did not manage to get the data from url ' + link[0]
        exit(1)
    return response


def any_text_is_there(patterns, text):
    t = text.lower().replace(' ', '')

    for ptrn in patterns:
        p = ptrn.lower()
        p = p.replace(' ', '')
        if re.search(p, t) is not None:
            return True
    return False


rs = 0


def get_link_from_page(page_addr, link_title):
    response = get_safe(page_addr)
    soup = BeautifulSoup(response.text, 'html.parser')
    res = ''
    for l in soup.find_all('a', href=True):
        if l.text == link_title:
            res = l['href']
            break
    return res


class HTMLResultsParser:

    def __init__(self, url, fmt, page_with_uiks, page_level):
        response = get_safe(url)
        page = BeautifulSoup(response.text, 'html.parser')

        self.url = url
        self.page = page

        parsed = urlparse(url)
        url_base = parsed.scheme + '://' + parsed.netloc

        self.url_base = url_base
        self.data_errors = []

        self.fmt = fmt
        self.page_with_uiks = page_with_uiks
        self.page_level = page_level
        self.page_with_link_to_uiks = False
        self.no_left_right_tables = False
        self.no_bottom_right_table = False
        self.one_candidate = False
        return

    def reset_data_errors(self):
        self.data_errors = []

    def parse_results_page(self, up_config, gs):

        found = False
        for table in self.page.find_all('table'):
            if re.search(u'Версия для печати', table.get_text()) is not None:
                found = True
                elections = self.parse_elections_results_table(table)
                break
        if found:
            self.data_errors = check_elections(elections, self.page_with_link_to_uiks, self.page_with_uiks,
                                               up_config, self.page_level, gs)

            if self.page_with_uiks:
                if u'УИК' not in elections['config']['commission']['title']:
                    subjects = elections['subjects']
                    if len(subjects) == 0:
                        print u'ERRORERROR0: parse_results_page: Did not find УИК in uik page title, title:'
                        print elections['config']['commission']['title']
                        print 'url:' + self.url
                        #exit(1)
                    found = False
                    # Sometimes UIKs are listed in bottom right table. In this case they are listed as subjects
                    # They must have no href in this case
                    for s in subjects:
                        if (u'УИК' not in s['title'] and u'Участок' not in s['title']) or 'href' in s:
                            found = True
                            break
                    if found:
                        print u'ERRORERROR1: parse_results_page: Did not find УИК in uik page title, url:'
                        print self.url
            else:
                if elections['config']['commission']['title'] is not None and u'УИК' in elections['config']['commission']['title']:
                    print u'ERRORERROR: parse_results_page: Found УИК in not uik page title, url:'
                    print self.url
                    exit(1)
            if up_config is None:
                elections['config']['data_errors'] = []
                for rec in self.data_errors:
                    elections['config']['data_errors'].append(rec)
            else:
                if 'data_errors' not in up_config:
                    up_config['data_errors'] = []

                for rec in self.data_errors:
                    up_config['data_errors'].append(rec)

            self.reset_data_errors()
        else:
            print 'ERRORERROR: did not find table with data (with Версия для печати)'
            exit(1)

        return elections

    def get_commission_href_and_title(self, table):
        rows = table.find_all('tr')
        for column in rows[0].find_all('td'):
            links = column.find_all('a', href=True)
            # There can be up to three links (federal->subject->tik)
            # Or for format 2 - yet another link to results of UIK (4-th)
            if ((len(links) < 1) or (len(links) > 3)) and \
                    (self.fmt != 2 and len(links) != 4):
                print 'info: badly formatted input, too many links to commission'
                raise LoadRetryWithDifferentFormat

            # If link to commission is relative, add scheme and site address
            parsed = urlparse(links[len(links) - 1]['href'])
            if parsed.netloc == '':
                href = urljoin(self.url_base, links[len(links) - 1]['href'])
            else:
                href = links[len(links) - 1]['href']

        return href, links[len(links) - 1].text.strip()

    def check_version_for_print(self, rows):
        columns = rows[2].find_all('td')
        if len(columns) != 1:
            print('info: badly formatted input, more than 1 column in row with print version')
            raise LoadRetryWithDifferentFormat

        for link in columns[0].find_all('a', href=True):
            if link['href'] != 'javascript:getXls();' or link.text != u'Версия для печати':
                print('info: badly formatted input, text in row with print version does is not expected')
                raise LoadRetryWithDifferentFormat
        return

    def check_tables_and_rows_len(self, tables):
        # Page level 1 for format 4 is empty, there are only links to UIKs
        if self.fmt == 4 and self.page_level == 1:
            return

        if self.fmt == 2:
            if self.page_with_link_to_uiks or self.page_with_uiks:
                check_len(tables, 7, 'info: badly formatted input, must 11 tables here')
            else:
                check_len(tables, 11,
                          'info: badly formatted input, must 11 tables here')  # 11 tables - format 2, 5 candidates (parliament, 2011)
        elif self.fmt in [0, 1, 4]:
            check_lens(tables, [7, 8, 9, 12], 'info: badly formatted input, must be 7, 8, 9 or 12 tables here')
        elif self.fmt == 3:
            check_len(tables, 7, 'info: badly formatted input, must be 7 tables here')
        elif self.fmt in [11, 16]:
            check_lens(tables, [0, 6, 7, 8, 10, 11, 12, 27, 25],
                       'info: badly formatted input, must be 6, 7, 8 or 10 tables here') #0
        elif self.fmt in [30, 17]:
            print 'WARN30'
            check_lens(tables, [3, 6, 8, 10], 'info: badly formatted input, must be 3, 6, 8 or 10 tables here')
        elif self.fmt in [10, 13, 15, 18, 5]:
            if self.fmt in [15, 5] and len(tables) == 0:
                return
            check_lens(tables, [3, 6, 7, 8, 10], 'info: must be 3, 6, 7 or 8 or 10 tables here')

        if self.fmt == 11 and len(tables) == 0:
            return
        if self.fmt in [12, 14] and len(tables) == 0:
            return
        elif len(tables) == 0:
            print 'info: Must be at least 1 table here: ' + self.url
            raise LoadRetryWithDifferentFormat
        rr = tables[0].find_all('tr')
        if self.fmt in [0, 3]:
            check_len(rr, 2, 'info: badly formatted input, must be 2 rows here')
        elif self.fmt in [1, 4]:
            check_len(rr, 3, 'info: badly formatted input, must be 3 rows here')
        elif self.fmt in [11, 16]:
            check_lens(rr, [2, 3], 'info: badly formatted input, must be 2 or 3 rows here')
        elif self.fmt in [30, 17]:
            print 'WARN30'
            check_lens(rr, [1, 2], 'info: badly formatted input, must be 1 or 2 rows here')
        elif self.fmt in [10, 13, 15, 18]:
            check_lens (rr, [1, 2], 'info: must be 1 or 2 rows here')
        elif self.fmt == 5:
            check_lens (rr, [1, 2, 3],  'info: must be 1, 2 or 3 rows here')
        elif self.fmt not in [2, 17, 16, 12, 14]:
            print 'ERROR! Unknown format specified: ' + str(self.fmt)

        if self.fmt in [0, 2, 3, 11, 16]:
            if len(tables) < 3:
                print 'info: Must be at least 3 tables here:'
                print self.url
                raise LoadRetryWithDifferentFormat()
            rr = tables[0].find_all('tr')
            cc = rr[0].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here')

            check_text(cc[0], HEADERS_AT_RESULTS_PAGE,
                       'info: badly formatted input, text does not correspond, text:' + cc[0].get_text() + " at " + self.url)
            cc = rr[1].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here 1')

            # Table 1 - date of voting, must be 1 row, 1 column
            rr = tables[1].find_all('tr')
            check_len(rr, 1, 'info: badly formatted input, must be 1 rows here')
            cc = rr[0].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here 2')

            # Table 2 - name of commission
            rr = tables[2].find_all('tr')
            check_len(rr, 3, 'info: badly formatted input, must be 3 rows here 3')
            cc = rr[0].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here 3')
            cc = rr[1].find_all('td')
            check_len(cc, 2, 'info: badly formatted input, must be 2 column here 4')
            cc = rr[2].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here 5')

            if self.fmt in [11, 16]:
                cc = rr[1].find_all('td')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: Text does not correspond')
            elif self.fmt in [0, 2, 3]:
                rr = tables[3].find_all('tr')
                check_len(rr, 1, 'info: badly formatted input, must be 1 rows here 14')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: badly formatted input, must be 1 column here 15')

            if self.fmt == 0:
                rr = tables[4].find_all('tr')
                check_len(rr, 1, 'info: badly formatted input, must be 1 rows here 16')
                cc = rr[0].find_all('td')
                check_len(cc, 2, 'info: badly formatted input, must be 2 columns here 17')

        elif self.fmt in [1, 4, 5, 30, 17, 12, 15, 18] or (self.fmt == 10 and self.page_level > 0) or \
                (self.fmt == 13 and self.page_level not in [0, 1] and not self.page_with_link_to_uiks):
            if self.fmt in [13, 17, 18] or (self.fmt == 10 and self.page_level > 0) \
                    or (self.fmt == 15 and self.page_level not in [0, 1]):
                rr = tables[0].find_all('tr')
                cc = rr[0].find_all('td')
                check_len(rr, 2, 'info: must be 2 rows here')
            elif self.fmt == 5 and self.page_level not in [0, 1]:
                rr = tables[0].find_all('tr')
                check_min_len(rr, 2, 'info: must be at least 2 rows here')
                cc = rr[1].find_all('td')
                check_len(rr, 3, 'info: must be 2 rows here')
            elif self.fmt == 30:
                print 'WARN30'
                rr = tables[0].find_all('tr')
                check_len(rr, 2, 'info: Must be 2 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 1, 'info: Must be 1 column here')

                rr = tables[2].find_all('tr')
                check_len(rr, 3, 'info: must be 3 rows here')
                cc = rr[1].find_all('td')
                check_text(cc[0], u'Наименование избирательной комиссии',
                           'info: badly formatted input, text does not correspond')

            if (self.fmt == 13 and self.page_level not in [0, 1] and not self.page_with_link_to_uiks):
                rr = tables[0].find_all('tr')
                cc = rr[1].find_all('td')
                check_len(rr, 2, 'info: Must be 2 rows here')
                check_len(cc, 1, 'info: Must be 1 column here')

                rr = tables[2].find_all('tr')
                if len(rr) < 2:
                    print 'info: must be at least 2 rows here'
                    raise LoadRetryWithDifferentFormat
                cc = rr[1].find_all('td')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')

            elif self.fmt not in [17, 18]:
                # Table 1 - title of elections
                rr = tables[1].find_all('tr')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: badly formatted input, must be 1 column here 6')
                check_len(rr, 1, 'info: badly formatted input, must be 1 column here 7')

            # Table 2 - name of commission and number of subjects
            rr = tables[2].find_all('tr')
            if len(rr) == 2:  # In parliament elections of 2003 there is 1 row here and couple of empty tables need to be skipped
                check_len(rr, 2, 'info: badly formatted input, must be 2 rows here')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: badly formatted input, must be 1 column here 8')
                cc = rr[1].find_all('td')
                check_len(cc, 1, 'info: badly formatted input, must be 1 column here 9')
                #            config['date'] = re.findall(r'(\d+.\d+.\d+)', cc[0].get_text())[0]

                # Table 3 - empty table
                rr = tables[3].find_all('tr')
                check_len(rr, 1, 'info: badly formatted input, must be 1 row here 10')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: badly formatted input, must be 1 column here 11')

                # Table 4 - empty table
                rr = tables[4].find_all('tr')
                check_len(rr, 1, 'info: badly formatted input, must be 1 row here 12')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: badly formatted input, must be 1 column here 13')
        return

    def get_titles(self, tables, config):
        # In format 11 some pages of level 1 contain direct links to UIKs pages (they are already marked as page_with_link_to_uiks
        # by this moment), if not - there must be drop down list with
        # list of links, each of them lead to separate pages with UIK. So format 19 consists of 3 and 4 hops from
        # top to UIKs
        if len(tables) == 0:
            return None, None
        rr = tables[0].find_all('tr')
        comm_title = None
        el_title = None
        cc = None

        if (self.fmt in [5, 10, 11, 17, 16, 12, 13, 15, 18]) or \
                (self.fmt == 14 and self.page_level in [0, 1, 2, 3, 5]):
            if self.fmt == 12:
                rr = tables[0].find_all('tr')
                if len(rr) < 2:
                    print 'info: Must be at least 2 rows here'
                    raise LoadRetryWithDifferentFormat
                cc = rr[1].find_all('td')
                check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[1].find_all('tr')
                check_len(rr, 1, 'info: must be 2 rows here')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: must be 1 column here')
                el_title = cc[0].get_text().strip()
            elif (self.page_level == 0 and self.fmt in [13, 18]):
                rr = tables[1].find_all('tr')
                if len(rr) < 2:
                    print 'info: Must be at least 2 rows here'
                    raise LoadRetryWithDifferentFormat
                cc = rr[1].find_all('td')
                check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[0].find_all('tr')
                check_len(rr, 2, 'info: must be 2 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 1, 'info: must be 1 column here')
                el_title = cc[0].get_text().strip()
            elif self.fmt == 14 and self.page_level in [0, 1, 2, 3, 5]:
                rr = tables[0].find_all('tr')
                if len(rr) < 2:
                    print 'info: Must be at least 2 rows here'
                    raise LoadRetryWithDifferentFormat
                cc = rr[1].find_all('td')
                if len(cc) == 1:
                    cc = rr[0].find_all('td')
                    check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
                    check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
                    comm_title = cc[1].get_text().strip()
                else:
                    check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
                    check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
                    comm_title = cc[1].get_text().strip()

                rr = tables[1].find_all('tr')
                check_len(rr, 1, 'info: must be 2 rows here')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: must be 1 column here')
                el_title = cc[0].get_text().strip()
            elif (self.page_level == 0 and self.fmt == 5) or \
                    self.fmt == 10 or self.fmt == 15:
                cc = rr[0].find_all('td')
                check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
                check_text(cc[0], u'Наименование Избирательной комиссии', 'info: text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[1].find_all('tr')
                check_len(rr, 1, 'info: must be 1 row here')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: must be 1 column here')
                el_title = cc[0].get_text().strip()
            elif self.page_level > 0 and self.fmt == 5:
                rr = tables[0].find_all('tr')
                if self.page_level == 1:
                    check_len(rr, 1, 'info: Must be 1 row here')
                    cc = rr[0].find_all('td')
                else:
                    check_len(rr, 3, 'info: Must be 3 rows here')
                    cc = rr[1].find_all('td')
                check_len(cc, 2, 'info: Must be 2 columns here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: Text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[1].find_all('tr')
                cc = rr[0].find_all('td')
                el_title = cc[0].get_text().strip()
            elif self.page_level == 0 and self.fmt == 17:
                rr = tables[1].find_all('tr')
                check_len(rr, 3, 'info: Must be 3 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 2, 'info: Must be 2 columns here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: Text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[0].find_all('tr')
                cc = rr[1].find_all('td')
                el_title = cc[0].get_text().strip()
            elif self.page_level in [1, 2] and self.fmt == 17:
                rr = tables[0].find_all('tr')
                check_len(rr, 2, 'info: Must be 2 rows here')
                cc = rr[0].find_all('td')
                check_len(cc, 2, 'info: Must be 2 columns here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: Text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[1].find_all('tr')
                cc = rr[0].find_all('td')
                el_title = cc[0].get_text().strip()
            elif self.fmt == 16:
                rr = tables[2].find_all('tr')
                check_len(rr, 3, 'info: Must be 3 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 2, 'info: Must be 2 columns here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: Text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[0].find_all('tr')
                cc = rr[1].find_all('td')
                el_title = cc[0].get_text().strip()
            elif self.fmt == 11 or (self.fmt == 13 and self.page_level in [1, 2, 3, 4]):
                rr = tables[2].find_all('tr')
                if len(rr) < 2:
                    print 'info: badly formatted input, must be not less than 2 rows here'
                    raise LoadRetryWithDifferentFormat
                cc = rr[1].find_all('td')
                check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
                comm_title = cc[1].get_text().strip()

                rr = tables[0].find_all('tr')
                check_len(rr, 2, 'info: must be 2 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 1, 'info: must be 1 column here')
                el_title = cc[0].get_text().strip()
        elif self.fmt == 30:
            if self.page_level == 0:
                print 'WARN30'
                rr = tables[0].find_all('tr')
                check_len(rr, 2, 'info: must be 2 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 1, 'info: must be 1 column here')
                el_title = cc[0].get_text().strip()

                rr = tables[1].find_all('tr')
                check_len(rr, 3, 'info: Must be 3 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 2, 'info: badly formatted input, must be 1 column here')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
                comm_title = cc[1].get_text().strip()
        elif self.fmt in [0, 2, 3]:
            rr = tables[2].find_all('tr')
            cc = rr[1].find_all('td')
            check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
            comm_title = cc[1].get_text().strip()
        elif self.fmt in [1, 4]:
            rr = tables[0].find_all('tr')
            if len(rr) < 2:
                print 'info: Must be at least 2 rows here'
                raise LoadRetryWithDifferentFormat
            cc = rr[1].find_all('td')
            #if len(cc) == 1:
            #    cc = rr[0].find_all('td')
            #    check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
            #    check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
            #    comm_title = cc[1].get_text().strip()
            #else:
            check_len(cc, 2, 'info: badly formatted input, must be 2 columns here')
            check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
            comm_title = cc[1].get_text().strip()

            rr = tables[1].find_all('tr')
            check_len(rr, 1, 'info: must be 1 row here')
            cc = rr[0].find_all('td')
            check_len(cc, 1, 'info: must be 1 column here')
            el_title = cc[0].get_text().strip()
        else:
            cc = rr[0].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here')

        if len(tables) == 0:
            return None, None
        if self.fmt in [0, 2, 3, 11, 16]:
            if len(tables) < 3:
                print 'info: Must be at least 3 tables here:'
                print self.url
                raise LoadRetryWithDifferentFormat()
            rr = tables[0].find_all('tr')
            cc = rr[0].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here')

            check_text(cc[0], HEADERS_AT_RESULTS_PAGE,
                       'info: badly formatted input, text does not correspond, text:' + cc[0].get_text())
            cc = rr[1].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here 1')
            el_title = cc[0].get_text()

            # Table 1 - date of voting, must be 1 row, 1 column
            rr = tables[1].find_all('tr')
            check_len(rr, 1, 'info: badly formatted input, must be 1 rows here')
            cc = rr[0].find_all('td')
            check_len(cc, 1, 'info: badly formatted input, must be 1 column here 2')
            config['date'] = re.findall(r'(\d+.\d+.\d+)', cc[0].get_text())[0]

            rr = tables[2].find_all('tr')

            if self.fmt in [11, 16]:
                cc = rr[1].find_all('td')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: Text does not correspond')
                comm_title = cc[1].get_text()
                if config['commission']['title'] is not None and config['commission']['title'] == '':
                    check_text(cc[1], config['commission']['title'], 'info: Text does not correspond')

        elif self.fmt in [1, 4, 30, 12, 15, 18] or (self.fmt == 10 and self.page_level > 0) or \
                (self.fmt == 13 and self.page_level not in [0, 1] and not self.page_with_link_to_uiks) or \
                (self.fmt == 5 and self.page_level > 1):
            if self.fmt == 5 and self.page_level not in [0, 1]:
                rr = tables[0].find_all('tr')
                check_len(rr, 3, 'info: must be at least 3 rows here')
                cc = rr[1].find_all('td')
                comm_title = cc[1].get_text()
            elif self.fmt == 30:
                print 'WARN30'
                rr = tables[0].find_all('tr')
                check_len(rr, 2, 'info: Must be 2 rows here')
                cc = rr[1].find_all('td')
                check_len(cc, 1, 'info: Must be 1 column here')
                el_title = cc[0].get_text()

                rr = tables[2].find_all('tr')
                check_len(rr, 3, 'info: must be 3 rows here')
                cc = rr[1].find_all('td')
                check_text(cc[0], u'Наименование избирательной комиссии',
                           'info: badly formatted input, text does not correspond')
                comm_title = cc[1].get_text()
            elif self.fmt not in [10, 13, 15, 18]:
                rr = tables[0].find_all('tr')
                cc = rr[1].find_all('td')
                check_len(rr, 3, 'info: must be 3 rows here')

            if self.fmt not in [13, 17, 10, 15, 18]:
                check_text(cc[0], u'Наименование Избирательной комиссии',
                           'info: badly formatted input, text does not correspond')
                if len(cc) < 2:
                    print 'info: must be at least 2 columns here'
                    raise LoadRetryWithDifferentFormat
                    comm_title = cc[1].get_text()

            if (self.fmt == 13 and self.page_level not in [0, 1] and not self.page_with_link_to_uiks) or (self.fmt == 18 and self.page_level > 0):
                rr = tables[0].find_all('tr')
                cc = rr[1].find_all('td')
                check_len(rr, 2, 'info: Must be 2 rows here')
                check_len(cc, 1, 'info: Must be 1 column here')
                el_title = cc[0].get_text()

                rr = tables[2].find_all('tr')
                if len(rr) < 2:
                    print 'info: must be at least 2 rows here'
                    raise LoadRetryWithDifferentFormat
                cc = rr[1].find_all('td')
                check_text(cc[0], u'Наименование избирательной комиссии', 'info: text does not correspond')
                comm_title = cc[1].get_text()

            elif self.fmt not in [17, 18]:
                # Table 1 - title of elections
                rr = tables[1].find_all('tr')
                cc = rr[0].find_all('td')
                check_len(cc, 1, 'info: badly formatted input, must be 1 column here 6')
                check_len(rr, 1, 'info: badly formatted input, must be 1 column here 7')
                el_title = cc[0].get_text()

        return comm_title, el_title

    def get_subjects(self, subjects, rows, title):
        # In format 11 some pages of level 1 contain direct links to UIKs pages (they are already marked as page_with_link_to_uiks
        # by this moment), if not - there must be drop down list with
        # list of links, each of them lead to separate pages with UIK. So format 11 consists of 3 and 4 hops from
        # top to UIKs
        if self.fmt in [0, 1, 2, 3]:
            return subjects

        if (self.page_level == 0 and self.fmt not in [4, 11, 12, 13, 14, 16, 30]) or \
            (self.fmt in [17, 18]) or \
                (self.fmt == 5 and self.page_level in [1, 2, 3, 4]) or \
                (self.fmt == 10 and self.page_level == 1) or \
                (self.fmt == 4 and self.page_level == 1) or \
                (self.fmt == 11 and self.page_level in [0, 1] and not self.page_with_link_to_uiks) or \
                (self.fmt in [12, 14] and self.page_level in [1, 2, 3] and not self.page_with_link_to_uiks) or \
                (self.fmt == 13 and self.page_level in [0, 1, 2] and not self.page_with_link_to_uiks) or \
                (self.fmt == 15 and self.page_level in [1, 3, 4]) :
            pass
            for r in rows:
                for form in r.find_all('form'):
                    for opt in form.find_all('option'):
                        ot = opt.get_text()
                        if ot != '---':
                            subjects.append({'id': len(subjects), 'title': ot, 'href': opt['value']})
                            if self.fmt == 14 and u'УИК' in ot:
                                subjects[len(subjects) - 1]['is_uiks_page'] = True
                    if self.fmt == 17:
                        if len(subjects) != 1:
                            print 'info: there must be 1 subject at this page'
                            raise LoadRetryWithDifferentFormat
                    elif self.fmt in [12, 13, 14]:
                        if len(subjects) == 0:
                            print 'info: there must be at least 1 subject at this page'
                            raise LoadRetryWithDifferentFormat
                    elif self.fmt in [5, 10, 11, 13, 15, 18]:
                        if len(subjects) < 1:
                            print 'info: there must be at least 1 subject at this page'
                            raise LoadRetryWithDifferentFormat
        elif self.fmt == 30 and self.page_level == 0:
            print 'WARN30'
            for r in rows:
                for form in r.find_all('form'):
                    for opt in form.find_all('option'):
                        ot = opt.get_text()
                        if ot != '---':
                            subjects.append({'id': len(subjects), 'title': ot, 'href': opt['value']})

                    if len(subjects) == 0:
                        print 'info: there must be not 0 subjects at this page'
                        raise LoadRetryWithDifferentFormat
        elif self.fmt == 16 and self.page_level == 1 and not self.page_with_link_to_uiks:
            subjects = self.look_for_uiks_links_on_subj_site(rows, title)

        return subjects

    def get_link_with_texts(self, href, texts):
        response = get_safe(href)
        soup = BeautifulSoup(response.text,'html.parser')
        res_href = ''
        links = soup.find_all('a', href=True)
        for text in texts:
            if res_href == '':
                for a in links:
                    if a.text.replace(' ', '').lower() == text.replace(' ', '').lower():
                        res_href = a['href']
                        break
        if res_href == '':
            print self.url
            print 'ERRORERROR: Did not find text "%s",  exiting' % text
            raise LoadRetryWithDifferentFormat
        return res_href

    def is_page_with_uiks(self, href):
        response = get_safe(href)
        soup = BeautifulSoup(response.text,'html.parser')
        tables = soup.find_all('table')
        for table in tables:
            rr = table.find_all('tr', recursive=True)
            for r in rr:
                cc = r.find_all('td', recursive=True)
                for c in cc:
                    if u'УИК' in c.get_text():
                        return True
        return False
    #for format 16 only
    def look_for_uiks_links_on_subj_site(self, rows, title):
        res = []
        subjects = []
        for r in rows:
            for form in r.find_all('form'):
                for opt in form.find_all('option'):
                    ot = opt.get_text()
                    if ot != '---':
                        res.append({'title': ot, 'href': opt['value']})
        for r in res:
            subj_page_href = self.get_link_with_texts(r['href'],
                                                     [u'сайт избирательной комиссии субъекта Российской Федерации'])
            # Add check about subj_page_href content. For some elections it already contains link to UIK results
            if self.is_page_with_uiks (subj_page_href):
                res_href = subj_page_href
            else:
                parsed = urlparse(subj_page_href)
                subj_url_base = parsed.scheme + '://' + parsed.netloc
                subj_page_href_1 = self.get_link_with_texts(subj_page_href, [title])
                subj_page_href_1 = subj_url_base + '/' + subj_page_href_1
                res_href = self.get_link_with_texts(subj_page_href_1, [u'Сводная таблица результатов выборов по единому округу'])
                #links = self.get_links(res_href)
                #if links is not None:
                #    res_href = links[len(subjects)]['href']
            subjects.append({'id': len(subjects), 'title': r['title'], 'href': res_href, 'is_uiks_page': True})
        return subjects


    #fmt 14 only
    def look_for_oiks_links_on_subj_site(self, href, title):
        subjects = []
        subj_page_href = self.get_link_with_texts(href, [title])
        parsed = urlparse(href)
        subj_url_base = parsed.scheme + '://' + parsed.netloc
        subj_page_href_1 = subj_url_base + '/' + subj_page_href
        res_href = self.get_link_with_texts(subj_page_href_1,
                                            [u'Итоги голосования (протокол №2)', u'Сводный отчет об итогах голосования'])
        return res_href

    def fill_subjects(self, config, subjects, tables, rows):

        if self.fmt == 4 and self.page_level == 1:
            links = self.get_links(self.url)
            for l in links:
                print '\t'*self.page_level + 'Got link: title: ' + l['title'] + ' href:' + l['href']
                subjects.append(
                    {'id': len(subjects), 'title': l['title'], 'href': l['href']})
        elif self.fmt == 11 and self.page_level in [0, 1] and len(tables) == 0:
            subjects = self.get_subjects(subjects, rows, config['commission']['title'])
            self.no_left_right_tables = True
        elif self.fmt in [12, 14] and self.page_level in [1, 3] and len(tables) == 0:
            subjects = self.get_subjects(subjects, rows, config['commission']['title'])
            self.no_left_right_tables = True
        elif self.fmt == 14 and self.page_level == 4:
            columns = rows[6].find_all('td')
            config['el_title'] = columns[0].get_text()
            if u'выборы' not in config['el_title'].lower():
                columns = rows[7].find_all('td')
                config['title'] = columns[0].get_text()
        elif self.fmt in [15, 5] and self.page_level in [1, 3, 4]:
            subjects = self.get_subjects(subjects, rows, config['commission']['title'])
            if len(tables) == 0:
                self.no_left_right_tables = True
        elif self.fmt == 13 and self.page_level == 1:
            subjects = self.get_subjects(subjects, rows, config['commission']['title'])
        else:
            if len(tables) == 0:
                print 'info: Must be at least 1 table here: URL:'
                print self.url
                raise LoadRetryWithDifferentFormat
            subjects = self.get_subjects(subjects, rows, config['commission']['title'])
        return subjects

    def process_top_table(self, table):
        config = {}
        subjects = []

        href, up_title = self.get_commission_href_and_title(table)
        config['commission'] = {'upper': href, 'href': self.url, 'title': up_title}

        # Second row (row 1) contains form with drop down list of lower level commissions, must be just one column.
        # Skip it
        # for tik commissions it contains link with results of uik - take it
        rows = table.find_all('tr')
        columns = rows[1].find_all('td')
        check_len(columns, 1, 'info: badly formatted input, more than 1 column in row with form with drop down list')
        rr = columns[0].find_all('tr')
        check_len(rr, 0, 'info: badly formatted input, this column must not contain rows')
        # If there is a link to UIKs results then this page is of tik level, not federal, subject or UIK
        # Another confirmation is that there is only left part of the page filled, with summaries.
        # No detalization on right. This is checked below - both conditions must be true simultaneously
        self.page_with_link_to_uiks = False
        links = columns[0].find_all('a', href=True)
        if len(links) == 1:
            if self.fmt == 14 and self.page_level in [1, 2]:
                subjects.append(
                    {'id': len(subjects), 'title': config['commission']['title'], 'href': links[0]['href']})
            else:
                self.page_with_link_to_uiks = True
                config['uiks_results'] = links[0]['href']

        self.check_version_for_print(rows)
        columns = rows[3].find_all('td')
        tables = columns[0].find_all('table')

        self.check_tables_and_rows_len(tables)
        subjects = self.fill_subjects(config, subjects, tables, rows)

        ct, el_title = self.get_titles(tables, config)
        if el_title is not None:
            config['el_title'] = el_title

        if ct is not None:
            if self.fmt in [4, 5, 12, 15]:
                ct = ct.replace('№ ', '№')
            if config['commission']['title'] is None or config['commission']['title'] == '':
                config['commission']['title'] = ct
            else:
                if config['commission']['title'] != ct:
                    if self.fmt in [2, 5, 10, 11, 13, 14, 18] and ct is not None:
                        config['commission']['title'] = config['commission']['title'] + ' ' + ct
                    elif self.fmt in [4, 17, 16, 12, 15] and ct is not None:
                        pass
                    else:
                        print 'config[commission][title] ', config['commission']['title']
                        print 'ct', ct
                        print 'url:'
                        print self.url
                        print exit(1)

        elections = {'config': config, 'subjects': subjects, 'title': config['commission']['title']}
        return elections

    def process_left_table(self, table, config):
        # left table - summary of results, column map and candidates
        rr = table.find_all('tr')
        cc = rr[0].find_all('td')
        if not (self.fmt == 14 and self.page_level in [1, 2, 3, 4, 5]):
            rows = rr[1:]
            if len(cc) != 2:
                print('info: badly formatted input, must be 2 columns here, URL: ')
                print self.url
                raise LoadRetryWithDifferentFormat
            if cc[1].text != u'Сумма':
                print('info: badly formatted input, must be Sum here')
                raise LoadRetryWithDifferentFormat
        else:
            if len(cc) == 2:
                if cc[1].text != u'Сумма':
                    print('info: badly formatted input, must be Sum here')
                    raise LoadRetryWithDifferentFormat
                rows = rr[1:]
            else:
                rows = rr[0:]

        config['columns_map'] = {}
        config['data_columns'] = []
        config['candidates_columns'] = []
        config['upper_level_stats'] = {}
        config['upper_level_candidates_stats'] = {}
        columns_map_to_process = True
        separator_found = False # There is a row with 2 columns between columns_map and candidates
        candidate_name = ''
        # Parse left table with column map and list of candidates
        for r in rows:
            cc = r.find_all('td')
            if columns_map_to_process:
                if (len(cc) == 3) and columns_map_to_process:
                    id = len(config['columns_map'])
                    config['columns_map'][str(id)] = cc[1].get_text().strip()
                    config['data_columns'].append(str(id))
                    if cc[2].get_text().strip() == '':
                        print 'info: Empty cells in left table, URL:'
                        print self.url
                        raise LoadFailedEmptyCells(self.url)
                    val = int(cc[2].get_text())
                    config['upper_level_stats'][str(id)] = val
                else:
                    columns_map_to_process = False
                    if self.fmt == 14 and len(cc) == 1:
                        if cc[0].get_text() in [u'Число голосов избирателей, поданных за',
                                                u'Число голосов избирателей, поданных за каждый список']:
                            separator_found = True

                if (len(config['columns_map']) == 0) and (not columns_map_to_process):
                    print 'ERROR! No columns found for columns_map. URL:'
                    print self.url
                    raise LoadRetryWithDifferentFormat

                if not columns_map_to_process:
                    if len(cc) == 2:
                        separator_found = True
            else:  # columns map processed, separator must be found by this time
                if not separator_found:
                    print 'ERROR, did not find separator after columns_map'
                    raise LoadRetryWithDifferentFormat

                if self.fmt == 14 and self.page_level in [1, 2, 3, 4, 5] and cc[0].get_text() == u'Против всех':
                    continue
                check_len(cc, 3, 'info: Must be 3 columns here')
                id = len(config['candidates_columns'])
                replaced = False
                if self.one_candidate:
                    # In case of one candidate elections first row contains name of the candidate and no digits
                    # Next - pro, another next - against. Take the name from the first and do not keep it in
                    # the list of the valid columns, replace with combined candidate_name + pro
                    # So if did not yet replace zero row which contained just candidate name - replace it
                    if id == 1 and config['columns_map']['c0'] == candidate_name:
                        id = 0
                        replaced = True
                    if candidate_name == '':
                        print 'Candidate name must be non empty here, exiting ... '
                        exit(1)
                    config['columns_map']['c' + str(id)] = candidate_name + ' - ' + cc[1].get_text().strip(' 0123456789,.')
                else:
                    config['columns_map']['c' + str(id)] = cc[1].get_text().strip(' 0123456789,.')
                if not replaced:
                    config['candidates_columns'].append('c' + str(id))
                    config['data_columns'].append('c' + str(id))
                t = cc[2].get_text()

                dd = re.findall(r'\d+\.?\d*', t)
                check_lens(dd, [0, 1,2], 'ERROR! Must be zero, one or two numbers for each candidate')
                # For elections with just one candidate one row contains his name, next contains "за", next "против"
                if len(dd) in [1, 2]:
                    config['upper_level_candidates_stats']['c' + str(id)] = int(dd[0])
                elif len(dd) == 0:
                    if self.fmt != 11:
                        print 'Must be digits here'
                        raise LoadRetryWithDifferentFormat
                    self.one_candidate = True
                    candidate_name = cc[1].get_text()
                    config['upper_level_candidates_stats']['c' + str(id)] = 0
        return config

    def process_bottom_left_table(self, table, config):
        # bottom left table - summary of the data about otkrepitelnie
        rr = table.find_all('tr')
        cc = rr[0].find_all('td')
        rows = rr[1:]
        if len(cc) != 2:
            print('info: badly formatted input, must be 2 columns here, URL: ')
            print self.url
            raise LoadRetryWithDifferentFormat
        if cc[1].text != u'Сумма':
            print(u'info: badly formatted input, must be Сумма here')
            raise LoadRetryWithDifferentFormat

        # Parse left table
        id = 0
        if 'tik_data' in config:
            print "ERROR: tik_data must be empty here!"
            exit(1)
        config['tik_data'] = {}
        config['tik_data']['titles'] = {}
        config['tik_data']['values'] = {}
        config['tik_data']['href'] = self.url
        config['tik_data']['page_level'] = self.page_level
        for r in rows:
            cc = r.find_all('td')
            if (len(cc) == 3):
                config['tik_data']['titles'][str(id)] = cc[1].get_text().strip()
                if cc[2].get_text().strip() == '':
                    print 'info: Empty cells in left table, URL:'
                    print self.url
                    raise LoadFailedEmptyCells(self.url)
                val = int(cc[2].get_text())
                config['tik_data']['values'][str(id)] = val
            else:
                print('ERRORERROR: badly formatted input, must be 2 columns here, URL: ')
                print self.url
                print "exiting"
                exit(1)
            id = id + 1
        return config


    def process_right_table(self, table, config, subjects):

        if self.fmt == 16 and self.page_level == 1:
            return subjects

        # in fmt 13 links from bottom right table leads to full set of subjects again so need to use those from top
        # in the future need to add getting subj data from bottom right table w/o getting links
        if self.fmt == 13 and self.page_level == 2 and len(subjects) > 0:
            return subjects

        if self.fmt in [15, 5] and self.page_level in [1, 3, 4] and len(subjects) > 0 and table is None:
            self.no_bottom_right_table = True
            return subjects

        if self.fmt == 14:
            if not self.page_with_uiks and self.page_level in [1, 2, 3, 4, 5] and len(subjects) != 0:
                return subjects
            elif self.page_with_uiks and self.page_level in [4, 5]:
                subjects.append(
                    {'id': len(subjects), 'title': config['commission']['title'], 'href': config['commission']['href']})
                for c in config['data_columns']:
                    if c in config['upper_level_candidates_stats']:
                        subjects[0][c] = config['upper_level_candidates_stats'][c]
                    else:
                        subjects[0][c] = config['upper_level_stats'][c]
                return subjects
            elif table is None and self.page_level in [2, 3]:
                return subjects
        if table is None:
            pass
        rr = table.find_all('tr')
        cc = rr[0].find_all('td')
        # format 30 don't contain data in right table, meaningful data are in the left table and are just
        # copied to results as if they were presented on right
        if (self.fmt == 30 and self.page_with_uiks) or \
                (self.fmt in [15, 5] and self.page_level == 2):
            # cc if filled with some trash for format 12
            if self.fmt == 30 and self.page_with_uiks:
                print 'WARN30'
                check_len(cc, 0, 'info: Subjects must be empty here')
            subjects.append({'id': len(subjects), 'title': config['commission']['title'], 'href': config['commission']['href']})
            for c in config['data_columns']:
                if c in config['upper_level_candidates_stats']:
                    subjects[0][c] = config['upper_level_candidates_stats'][c]
                else:
                    subjects[0][c] = config['upper_level_stats'][c]

        else:
            nsubjects = len(cc)
            # nsubjects can be zero only for TIK tables
            if nsubjects == 0 and not self.page_with_link_to_uiks:
                self.no_bottom_right_table = True
                # For 11 format instead of tik level there can be list of links to UIKs in drop down list
                # on top of page, they should be in 'subjects'
                if self.fmt in [11, 13] and self.page_level == 1:
                    if (subjects is None) or (len(subjects) == 0):
                        print 'info: subjects must be filled here'
                        raise LoadRetryWithDifferentFormat
                    else:
                        return subjects
                else:
                    print 'info: no subjects in not TIK table here:'
                    print self.url
                    raise LoadRetryWithDifferentFormat

            # Take subjects from bottom right page, skip links to the same subjects coming from top.
            # TODO: compare that they are same subjects (by hrefs and titles), do same for other formats.
            if self.fmt == 5 and len(subjects) > 0 and len(subjects) == nsubjects:
                subjects = []

            use_upper_subj_hrefs = False
            if self.fmt in [10, 11, 15] and len(subjects) > 0 and len(subjects) == nsubjects:
                use_upper_subj_hrefs = True

            if not self.page_with_link_to_uiks:
                # first row contains names of subjects and links to appropriate commissions sites
                # for tables with uiks on subjects sites no links - uiks do not have sites
                if self.page_with_uiks:
                    for c in cc:
                        subjects.append(
                            {'id': len(subjects), 'title': c.get_text().strip()})
                else:

                    if self.fmt == 14 and self.page_level == 0:
                        print 'Getting links to ' + str(len(cc)) + ' subjects'
                        for c in cc:
                            links = c.find_all('a', href=True)
                            if len(links) != 1:
                                pass
                            check_len(links, 1, 'info: must be 1 link in column here: ' + self.url)
                            href = self.look_for_oiks_links_on_subj_site(links[0]['href'], links[0].text)
                            subjects.append({'id': len(subjects), 'title': links[0].text, 'href': href})
                            print 'Got link to ' + links[0].text
                    elif not (self.fmt == 11 and self.page_level in [0, 1]) and \
                            not (self.fmt == 16 and self.page_level == 1) and \
                            not (self.fmt in [12, 13, 14] and self.page_level == 1 and len(subjects) > 0):

                        nsubj = 0
                        for c in cc:
                            links = c.find_all('a', href=True)
                            if len(links) != 1:
                                if self.fmt == 10 and self.page_level == 1 and len(subjects) > 0:
                                    return subjects
                                if self.fmt == 15 and len(subjects) > 0:
                                    return subjects
                            check_len(links, 1, 'info: must be 1 link in column here: ' + self.url)

                            if not use_upper_subj_hrefs:
                                subjects.append({'id': len(subjects), 'title': links[0].text, 'href': links[0]['href']})
                            else:
                                if links[0].text not in subjects[nsubj]['title']:
                                    print 'ERRORERROR2: Something is wrong, subj title must be substring here'
                                    exit(1)
                            nsubj += 1

                id = 0
                cid = 0
                break_found = False
                def key(href):
                    query = urlparse(href)[4].split('&')
                    query = dict(item.split("=") for item in query)
                    k = ""
                    for key, value in sorted(query.iteritems()):
                        if key not in ['root','global','tvd','type']:
                            k += key + value
                    return k
                # Iterate over the first row to get names of subjects and hrefs
                if use_upper_subj_hrefs:
                    cc = rr[0].find_all('td')
                    l_id = 0
                    lower_subjs = {}
                    di = {}
                    for c in cc:
                        c1 = cc[l_id]
                        a = c1.find_all('a')
                        a = a[0]
                        st = key(a['href'])
                        lower_subjs[st] = l_id
                        di[l_id] = st
                        l_id += 1
                skipped_first_row = False
                for r in rr[1:]:
                    cc = r.find_all('td')
                    # first part of the table prior to separator
                    if not break_found:
                        if id != len(config['columns_map']) - len(config['candidates_columns']):
                            if len(cc) != nsubjects:
                                pass
                            check_len(cc, nsubjects, 'info: number of columns is not equal ' +
                                                     'to number of subjects here: ' + str(len(cc)) +
                                      ' columns ' + str(nsubjects) + ' subjects')
                            subj = 0
                            if use_upper_subj_hrefs:
                                for subj in subjects:
                                    st = key(subj['href'])
                                    i = lower_subjs[st]
                                    subj[str(id)] = int(cc[i].get_text())
                            else:
                                for c in cc:
                                    try:
                                        subjects[subj][str(id)] = int(c.get_text())
                                        subj += 1
                                    except:
                                        raise LoadRetryWithDifferentFormat
                            id += 1
                        else:
                            check_len(cc, 2, 'info: expected separator of 2 columns')
                            break_found = True
                    #second part of the table after the separator
                    else:
                        check_len(cc, nsubjects, 'info: must be ' + str(nsubjects) + ' columns here')
                        subj = 0
                        # Skip one row in case of one candidate - it contains name of the candidate but no digits
                        if self.fmt == 11 and self.one_candidate and cid == 0 and not skipped_first_row:
                            skipped_first_row = True
                            continue
                        if use_upper_subj_hrefs:
                            for subj in subjects:
                                st = key(subj['href'])
                                i = lower_subjs[st]
                                t = cc[i].get_text()
                                dd = re.findall(r'\d+\.?\d*', t)
                                subj['c' + str(cid)] = int(dd[0])
                        else:
                            for c in cc:
                                t = c.get_text()
                                dd = re.findall(r'\d+\.?\d*', t)
                                subjects[subj]['c' + str(cid)] = int(dd[0])
                                subj += 1
                        cid += 1
        return subjects

    def process_bottom_right_table(self, table, config, subjects):
        rr = table.find_all('tr')
        nsubjects = len(subjects)
        config['tik_data']['subjects'] = []

        for i in range(nsubjects):
            config['tik_data']['subjects'].append({})

        # Calculate number of the next column to add the data to
        id = 0
        for r in rr[1:]:
            cc = r.find_all('td')
            if len(cc) != nsubjects:
                print "ERROR: Number of columns is not equal to the number of subjects, stopped processing the table"
                return config

            check_len(cc, nsubjects, 'info: number of columns is not equal ' +
                      'to number of subjects here: ' + str(len(cc)) +
                      ' columns ' + str(nsubjects) + ' subjects')
            subj = 0
            rec = {}
            for c in cc:
                try:
                    config['tik_data']['subjects'][subj][str(id)] = int(c.get_text())
                    subj += 1
                except:
                    #raise LoadRetryWithDifferentFormat
                    exit(1)
            id += 1
        return config

    def find_left_table(self, tables):

        inner_t = -1
        found = False
        tn = 0
        for t in range(len(tables)):
            tn += 1
            if not found:
                rr = tables[t].find_all('tr', recursive=False)
                for r in rr:
                    if not found:
                        cc = r.find_all('td', recursive=False)
                        for c in cc:
                            if not found and c.get_text() == u'Сумма':
                                inner_t = t
                                found = True
                                break
        if inner_t == -1 and self.fmt == 14:
            for t in range(len(tables)):
                if not found:
                    for r in tables[t].find_all('tr', recursive=False):
                        if not found:
                            for c in r.find_all('td', recursive=False):
                                if not found and u'Итоги голосования'.lower() in c.get_text().strip(' ').lower():
                                    inner_t = t + 2
                                    found = True
                                    break

        if inner_t == -1:
            if self.page_with_uiks:
                print u'info: badly formatted input, did not find table with Сумма or Итоги голосования, URL:'
                print self.url
                print 'LoadFailedEmptyCells'
                raise LoadFailedEmptyCells(self.url)
            else:
                return inner_t

        if self.fmt != 14:
            rr = tables[inner_t].find_all('tr')
            cc = rr[0].find_all('td')
            check_text(cc[1], u'Сумма', u'info: Text does not correspond, must be Сумма here')

        nt = inner_t - 1

        return nt

    def find_bottom_left_table(self, tables):
        # Look for the second table with Сумма
        inner_t = -1
        found = False
        tn = 0
        for t in range(len(tables)):
            tn += 1
            rr = tables[t].find_all('tr', recursive=False)
            for r in rr:
                cc = r.find_all('td', recursive=False)
                for c in cc:
                    txt = c.get_text()
                    if not found:
                        if c.get_text() == u'Сумма':
                            found = True
                    else:
                        if c.get_text() == u'Сумма':
                            inner_t = tn - 1
                            return inner_t

        return inner_t


    def parse_elections_results_table(self, table):
        elections = self.process_top_table(table)
        comm_title = elections['title']

        if (self.fmt in [5, 10, 13, 15, 17, 18, 30] and self.page_level == 0) or \
                (self.fmt == 4 and self.page_level == 1) or \
                (self.fmt == 11 and self.page_level == 1 and self.no_left_right_tables) or \
                (self.fmt in [5, 12, 14, 15] and self.page_level == 1 and self.no_left_right_tables):
            return elections
        else:
            rows = table.find_all('tr')
            # Fourth row - сводная таблица результатов выборов
            columns = rows[3].find_all('td')
            # Sometimes elections page contain information about 'otkrepitelnie' (4 additional tables)
            tables = columns[0].find_all('table')

            nt = self.find_left_table(tables)
            if nt == -1 and self.fmt == 13:
                self.no_left_right_tables = True
                return elections

            if nt == -1:
                self.no_left_right_tables = True
                return elections

            # Summary of results and column map (left table) and summary by subjects (right table)
            tbls = tables[nt].find_all('table')

            if nt != -1 and self.fmt == 5 and tbls == []:
                tbls = [tables[nt], tables[nt + 1]]
            # Dirty hack for 2003_subject_mayor_город_Санкт-Петербург_Выборы_высшего_должностного_лица_Санкт-Петербурга_-губернатора_Санкт-Петербурга.
            # There must be two tables here, left with summary and right with details.
            # But right table is not used for page_with_link_to_uiks anyways
            # And there are elections where format is broken at this pages but it's a pity to kick them out
            # 2003_subject_mayor_город_Санкт-Петербург_Выборы_высшего_должностного_лица_Санкт-Петербурга_-губернатора_Санкт-Петербурга.
            skip_check_and_processing = False
            elections_exceptions = [
                u'Выборы высшего должностного лица Санкт-Петербурга -губернатора Санкт-Петербурга. Повторное голосование',
                u'Выборы Губернатора Вологодской области'
            ]
            if 'el_title' in elections['config'] and elections['config']['el_title'] in elections_exceptions \
                    and (self.page_with_link_to_uiks or self.page_level == 2) and self.fmt in [12, 17, 16] and len(tbls) == 1:
                skip_check_and_processing = True

            if self.fmt == 14 and self.page_level in [1, 2, 3, 4, 5] and len(tbls) == 0:
                tbls = [tables[nt], None]
            elif self.fmt == 14 and self.page_level == 4 and len(tbls) == 1:
                tbls = [tbls[0], None]
            elif self.fmt in [15, 5] and self.page_level == 1 and len(tbls) == 1:
                tbls = [tbls[0], None]
            elif len(tbls) < 2 and not skip_check_and_processing:
                print 'WARNING! Must be not less than 2 tables here (results summary and details), url:'
                print self.url
                raise LoadRetryWithDifferentFormat

            config = self.process_left_table(tbls[0], elections['config'])
            if not skip_check_and_processing:
                subjects = self.process_right_table(tbls[1], elections['config'], elections['subjects'])
            else:
                subjects = elections['subjects']

            blt = self.find_bottom_left_table(tables)
            if blt != -1:
                config = self.process_bottom_left_table(tables[blt], elections['config'])
                config = self.process_bottom_right_table(tables[blt + 1], config, elections['subjects'])

            elections = {'config': config, 'subjects': subjects, 'title': comm_title}
        return elections


    #for format 4 only
    def get_links (self, link):
        response = get_safe(link)
        soup = BeautifulSoup(response.text,'html.parser')
        res = []
        for form in soup.find_all('form'):
            for opt in form.find_all('option'):
                ot = opt.get_text()
                if ot != '---':
                    res.append({'title':ot, 'href': opt['value']})
        if len(res) == 0:
            print 'info: there must be links at this page'
            raise LoadRetryWithDifferentFormat
        return res
