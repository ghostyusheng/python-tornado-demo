import datetime
import re

from share.utils.langconv import Converter


def suggest_search_amend(kw):
    """
    ios user keyboard search:
        w z r y -> wzry 
    """
    lst = kw.strip().split(" ")
    for ele in lst:
        print(ele)
        if (len(ele) != 1) or (not ele.isalnum()):
            return kw
        if '\u4e00' <= ele <= '\u9fa5':
            return kw
    kw = kw.replace(" ", "")
    return kw


def filterSpecialChar(s, char=' '):
    return re.sub('[\\\.\!\/_,$%^*()+\"\'+|\[\]+——！，。？?\u2006、~@#￥%……&*（）:：<>《》{}【】¥;"“」`「～\-\f\n\r\t\v]+', char, s).strip()


def getBeforDate(beforeOfDay, _format="%Y-%m-%d"):
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-beforeOfDay)
    re_date = (today + offset).strftime(_format)
    return re_date

def minDistance(word1, word2):

    if not word1:
        return len(word2 or '') or 0

    if not word2:
        return len(word1 or '') or 0

    size1 = len(word1)
    size2 = len(word2)

    last = 0
    tmp = list(range(size2 + 1))
    value = None

    for i in range(size1):
        tmp[0] = i + 1
        last = i
        for j in range(size2):
            if word1[i] == word2[j]:
                value = last
            else:
                value = 1 + min(last, tmp[j], tmp[j + 1])
            last = tmp[j + 1]
            tmp[j + 1] = value
    return value


def t2s(text):
    return Converter('zh-hans').convert(text)


shuzi2num = {
    '零': 0,
    '一': 1,
    '二': 2,
    '三': 3,
    '四': 4,
    '五': 5,
    '六': 6,
    '七': 7,
    '八': 8,
    '九': 9,
    '十': 10
}
before_end = ['老','之','第','合','零','一','二','三','四','五','六','七','八','九','十']


def transNumSuffix(text):
    if text and len(text) > 1:
        second_last = text[-2]
        end = text[-1]
        if (end in shuzi2num) and (second_last not in before_end):
            text = text[0:-1] + str(shuzi2num[end])
    return text


if __name__ == "__main__":
    print(transNumSuffix("少年三国志二"))
    print(transNumSuffix("崩坏三"))
    print(transNumSuffix("百分之一"))
    print(transNumSuffix("先锋老三"))
    print(transNumSuffix("妃十三"))
    print(transNumSuffix("三十三"))
    print(transNumSuffix("十"))
    print(transNumSuffix(None))
    print(transNumSuffix(''))
