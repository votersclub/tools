#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import codecs
from time import sleep
import re

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

PATTERNS_TO_EXCLUDE = [u'одномандатн', u'мажоритарн']

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
    if s == '':
        print error_message
        raise LoadRetryWithDifferentFormat


def make_link (href, title):
    return "<a href=\'" + href + "\'>" + title + "</a>"

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
            max_retries = 7
            while n_retries < max_retries and bad_page:
                bad_page = False
                sleep(1)
                response = requests.get(link, headers, timeout=5)
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


class HTMLListParser:

    def __init__(self):
        return

    def get_level(self, table):
        ss = table.find_all('select')
        level = None
        for s in ss:
            if s.attrs['name'] == 'urovproved':
                options = s.find_all('option')
                for opt in options:
                    if 'selected' in opt.attrs:
                        t = opt.get_text().lower()
                        if t in levels:
                            if level is None:
                                level = levels[t]
                            else:
                                print 'ERRORERROR: Cannot work with several elections levels simultaneously, please select one'
                                exit(1)
        return level

    def parse_elections_list_file(self, file):
        f = codecs.open(file, encoding='windows-1251')
        d = f.read()
        soup = BeautifulSoup(d, 'html.parser')
        f.close()
        take_next = False
        for table in soup.find_all('table'):
            if re.search(u'Уровень выборов', table.get_text()) is not None:
                if len(table.find_all('table')) == 0:
                    level = self.get_level(table)

            if take_next:
                elections_list = self.parse_elections_list_table(table)
                if level is None or level == '':
                    print('ERRORERROR: No level for elections list')
                    exit(1)
                elections_list['level'] = level
                return elections_list
            # find the innermost table with this text
            if re.search(u'Всего найдено записей:', table.get_text()) is not None:
                if len(table.find_all('table')) == 0:
                    take_next = True
        return None

    def get_results_href(self, href):
        response = get_safe(href)
        soup = BeautifulSoup(response.text, 'html.parser')
        global rs
        rs += 1
        if rs == 24:
            pass

        results_hrefs = []
        for table in soup.find_all('table'):
            if any_text_is_there(PATTERNS_LINKS_TO_RESULT_DATA, table.get_text()):
                # Look for innermost table with ^^ header
                if len(table.find_all('table')) == 0:
                    rr = table.find_all('tr')
                    for r in rr:
                        cc = r.find_all('td')
                        check_lens(cc, [1,2], 'Must be 1 or 2 columns here, exiting')
                        for c in cc:
                            if any_text_is_there(PATTERNS_LINKS_TO_RESULT_DATA, c.get_text()):
                                links = c.find_all('a', href=True)
                                check_len(links, 1, 'Must be 1 link here')
                                for l in links:
                                    results_hrefs.append ({'href':l['href'], 'title':l.text})

        # If empty - exception
        if len(results_hrefs) == 0:
            print 'Did not find results_href, ERRORERROR'
            raise LoadFailedDoNotRetry(href)
        # If one link - return it
        elif len(results_hrefs) == 1:
            return results_hrefs[0]['href']
        # If there are several protocols (links), try to return one which does not contain patterns to exclude
        # If did not manage, return the last one (it usually contains links to results we need)
        else:
            for r in results_hrefs:
                if not any_text_is_there(PATTERNS_TO_EXCLUDE, r['title']):
                    return r['href']
            return results_hrefs[len(results_hrefs) - 1]['href']

        return None

    def parse_elections_list_row(self, elections_list, rr, n_filtered_out, nr):
        global nrec
        global no_results_href
        # date
        cc = rr[nrec].find_all('td')
        check_len(cc, 1, 'Must be 1 column here')
        dt = cc[0].get_text().strip()

        nrec += 1
        cc = rr[nrec].find_all('td')
        region = ''
        while (nrec < nr) and (len(cc) == 2):
            if cc[0].get_text().strip() != '':
                region = cc[0].get_text().strip()

            if region == '':
                print 'ERRORERROR: Empty region, exiting'
                exit(1)

            links = cc[1].find_all('a', href=True)
            href = links[0]['href']
            title = links[0].text.strip()

            print 'Region: ' + region + ' title: ' + title + ' date: ' + dt + ', row ' + str(nrec) + ' out of ' + str(nr)

            try:
                results_href = self.get_results_href(href)
                rec = {'date': dt, 'generic_href': href, 'results_href': results_href, 'title': title, 'location': region}

            except LoadFailedDoNotRetry:
                print 'ERROR: Exception at row number ' + str(nrec) + ' did not get results href, writing NULL'
                print 'Generic URL: ' + href
                rec = {'date': dt, 'generic_href': href, 'results_href': 'NULL', 'title': title, 'location': region}
                no_results_href += 1
            except LoadRetryWithDifferentFormat:
                print 'WARNING: Exception at row number ' + str(nrec) + ' did not get results href, writing NULL'
                print 'Generic URL: ' + href
                rec = {'date': dt, 'generic_href': href, 'results_href': 'NULL', 'title': title, 'location': region}
                no_results_href += 1
            elections_list['elections'].append(rec)

            nrec += 1
            if nrec < nr:
                cc = rr[nrec].find_all('td')

        return n_filtered_out

    def parse_elections_list_table(self, table):
        elections_list = {'elections': []}

        # First row - date, next row - region, title and link
        rr = table.find_all('tr')
        nr = len(rr)
        global nrec
        n_filtered_out = 0
        nexceptions = 0
        nrec = 0
        while nrec < nr:
            try:
                n_filtered_out = self.parse_elections_list_row(elections_list, rr, n_filtered_out, nr)
            except LoadFailedDoNotRetry:
                print 'WARNING: Exception, skipped row number ' + str(nrec)
                nexceptions += 1
                nrec += 1
            except LoadRetryWithDifferentFormat:
                print 'WARNING: Exception, skipped row number ' + str(nrec)
                nexceptions += 1
                nrec += 1

        print 'Returning ' + str(len(elections_list['elections'])) + ', filtered out ' + \
              str(n_filtered_out) + ', exceptions: ' + str(nexceptions) + ' no_results_href: ' + str(no_results_href)
        print ' Total (taken + exceptions + filtered out): ' + \
              str(len(elections_list['elections']) + n_filtered_out + nexceptions)
        return elections_list
