# -*- coding: utf-8 -*-

import datetime
import re
from urllib.parse import parse_qs, urlencode

import time
from numpy import linalg

from core.const import const
from function.context import context_var
from function.function import R
from service.log import LogService
from share.utils.utils import filterSpecialChar
from utils import common
from utils.const import TITLE_FILTERS, KEYWORD_FILTERS


def getDateStr():
    date = datetime.datetime.now()
    return date.strftime('%Y%m%d')


def str_toDatetime(str):
    return datetime.datetime.strptime(str, '%Y-%m-%d %H:%M:%S')


def get_seconds(date):
    return int((date - datetime.datetime(1970, 1, 1)).total_seconds())


def seconds_todate(seconds):
    timeArray = time.localtime(seconds)
    return time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

def seconds_tohour(seconds):
    timeArray = time.localtime(seconds)
    return time.strftime("%H", timeArray)


def log_exception(e):
    request = R()
    user_id = 'GUEST'
    if request:
        user_id = request.getUserId()
    LogService.instance().user(user_id).setLogName(
        'exception').addFileHandler().exception(e)
    #const.SENTRY.capture_message(e)


def index_exception(e):
    request = R()
    user_id = request.getUserId() or 'GUEST'
    LogService.instance().user(user_id).setLogName(
        'index').addFileHandler().exception(e)


def str_is_identifier(str):
    if re.match('^([A-Za-z]{1}[A-Za-z\d_]*\.)*[A-Za-z][A-Za-z\d_]*$', str):
        return True
    return False


def syslog(*msgs):
    request = R()
    user_id = request.getUserId() or 'GUEST'
    msg = ' ~~ '.join(map(lambda a: str(a), msgs))
    msg += ' ({}s)'.format(round(time.time() - context_var.get().HTTP_START_TS, 3))
    LogService.instance().user(user_id).setLogName('sys').addFileHandler().info(msg)


def eslog(msg, typ):
    pass


def ouDistance(a, b):
    dist = linalg.norm(a - b)
    sim = 1.0 / (1.0 + dist)  # 归一化
    return sim


def getBeforDate(beforeOfDay, _format="%Y-%m-%d"):
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-beforeOfDay)
    re_date = (today + offset).strftime(_format)
    return re_date


def getCurrentDate():
    date = datetime.datetime.now()
    return date.strftime('%Y-%m-%d')


def normalization(text):
    pattern = "|".join(map(re.escape, common.NORMALIZATION.keys()))
    return re.sub(re.compile(pattern), lambda m: common.NORMALIZATION[m.group()], text)


def stripTags(html):
    dr = re.compile(r'<[^>]+>', re.S)
    return dr.sub('', html)


def wrapHtml(s, mark, label):
    return s.replace(mark, str(label).format(mark))


def capitalize(string, lower_rest=False):
    return string[:1].upper() + (string[1:].lower() if lower_rest else string[1:])


def getDateHourStr():
    date = datetime.datetime.now()
    return date.strftime('%Y%m%d%H')


def clearTitle(kw):
    origin_kw = kw
    kw = kw.encode('UTF-8', 'ignore').decode('UTF-8')
    symbols = ['(', '（', '[', '「', '【', '-', '·', ':', '：', '（']
    clear = False
    for s in symbols:
        pos = kw.find(s)
        if pos == -1:
            continue
        kw = kw[:pos].strip()
        clear = True
    replace_reg = re.compile(r'[0123456789零一二三四五六七八九I]$')
    result = replace_reg.sub('', kw)
    result = filterSpecialChar(result)
    result = keywordFilter(result)
    if result != origin_kw:
        clear = True
    return result, clear


def titleFilter(kw):
    symbols = TITLE_FILTERS
    for symbol in symbols:
        if kw.endswith(symbol):
            kw = kw.replace(symbol, '')
            break
    return kw


def keywordFilter(kw):
    kw = kw.encode('UTF-8', 'ignore').decode('UTF-8')
    filters = KEYWORD_FILTERS
    for filter in filters:
        if kw.endswith(filter):
            kw = kw.replace(filter, '')
            break
    return kw



def parseSearchParams(s):
    query = s.split("#")[1]
    params = parse_qs(query)
    return params


def buildSearchParams(data, prefix="SEARCH"):
    return prefix + "@" + urlencode(data, doseq=True)


def buildLogVia(items, fro, prefix=''):
    for pos, item in enumerate(items):
        params_dict = {
            "pos": pos+fro,
            "strategy": item.get('strategy', '')
        }
        log_via = buildSearchParams(params_dict)
        item['logVia'] = prefix + log_via


def getInnerFieldValue(hit, fields):
    pre = hit
    for ix, field in enumerate(fields):
        pre = pre.get(field)
        if pre is None:
            return 0
        if ix == len(fields)-1:
            return int(pre)


def sortByField(hits, field, order):
    if hits and field and order:
        if order == "desc":
            reverse = True
        elif order == "asc":
            reverse = False
        else:
            raise Exception("parameter order should be 'desc' or 'asc' ")
        fields = field.split(".")
        hits = list(sorted(hits, key=lambda x: getInnerFieldValue(
            x, fields), reverse=reverse))
    return hits


def standardization(data, max, min):
    _range = max - min
    return (data - min) / _range


def getHighlightTokens(keywords):
    tokens = []
    current_token = ''
    for i in filterSpecialChar(keywords):
        if i.encode('UTF-8').isalpha():
            current_token += i
            continue

        if current_token != '':
            tokens.append(current_token)
            current_token = ''
        tokens.append(i)
    if current_token != '':
        tokens.append(current_token)
    return tokens


def chooseMatchLanguageTitle(kw, default_title, title_all, alias, pinyin, abbr, title_lan):
    if not title_all:
        return default_title
    sim_dict = dict()
    kw = kw.lower()
    contains_titles = []
    for title in title_all:
        if kw in title.lower():
            contains_titles.append(title)
            continue
        kw_char_set = set(kw)
        title_char_set = set(title.lower())
        sim_score = len(kw_char_set & title_char_set) / \
            len(kw_char_set | title_char_set)
        sim_dict[title] = sim_score
    title_sims = list(
        sorted(sim_dict.items(), key=lambda x: x[1], reverse=True))
    if contains_titles:
        if not title_lan:
            return contains_titles[0]
        if len(contains_titles) == 1:
            return contains_titles[0]
        else:
            if 'zh_CN' not in title_lan:
                return contains_titles[0]
            title_cn = title_lan['zh_CN']
            if title_cn and (title_cn in contains_titles):
                return title_cn
            else:
                return contains_titles[0]
    # 匹配title相似度很低 说明输入的是pinyin 或者 拼音的缩写  这时取中文title
    if alias:
        for name in alias:
            if kw in name.lower():
                return default_title

    if pinyin and (kw in pinyin):
        return default_title

    if abbr and (kw in abbr):
        return default_title

    if title_sims[0][1] < 0.2:
        return default_title
    return title_sims[0][0]



def filterKeywordByCommonStopWord(keyword: str):
    replace_reg = re.compile(
        r"[的|了|在|是|有|和|就|不|人|都|一|一个|上|也|很|到|说|要|去|会|着|看|好|这|之]")
    result = replace_reg.sub('', keyword)
    return result


def containChinese(s: str):
    for _char in s:
        if '\u4e00' <= _char <= '\u9fa5':
            return True
    return False


def analyzedWordIsAnApp(word: dict):
    black_list = ['手游', '先行服', "先行版", "测试服", "测试版", "攻略", "游戏"]
    title = word.get('word')
    nature = word.get('nature')
    return (nature == 'app') and (len(title) > 1) and (title not in black_list)
