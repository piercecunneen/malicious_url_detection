# functions to produce features for a url
'''
length of url
delimeter count
number of non-alphanumeric characters
length of parameter section
digit count in host
non-alphanumeric count in host
'''

import json
from urlparse import urlparse

def count_case_changes(s):
    changes = 0
    lower = False
    for i in s:
        if i.isupper() and lower:
            lower = False
            changes += 1
        elif i.islower() and not lower:
            lower = True
            changes += 1
    return changes

def count_digits(s):
    c = 0
    c += s.count('0')
    c += s.count('1')
    c += s.count('2')
    c += s.count('3')
    c += s.count('4')
    c += s.count('5')
    c += s.count('6')
    c += s.count('7')
    c += s.count('8')
    c += s.count('9')
    return c

def get_counts(s):
    return [
        len(s),
        s.count('@'),
        s.count('.'),
        s.count('?'),
        s.count('-')
    ]

def create_features(url):
    o = urlparse(url)
    features = list()
    features.append(url)
    features += get_counts(url)
    host = o.netloc
    features += get_counts(host)
    path = o.path
    features += get_counts(path)
    query = o.query
    features += get_counts(query)
    features.append(count_digits(host))
    features.append(count_case_changes(path))
    features.append(count_case_changes(query))
    non_alpha = sum(c.isalnum() for c in url)
    features.append(non_alpha)

    return [
            'url',
            'url length',
            '# of @s in url',
            '# of .s in url',
            '# of ?s in url',
            '# of -s in url',
            'host length',
            '# of @s in host',
            '# of .s in host',
            '# of ?s in host',
            '# of -s in host',
            'path length',
            '# of @s in path',
            '# of .s in path',
            '# of ?s in path',
            '# of -s in path',
            'query length',
            '# of @s in query',
            '# of .s in query',
            '# of ?s in query',
            '# of -s in query',
            '# of digits in hostname',
            '# of case changes in path',
            '# of case changes in query',
            '# of non alphanumeric characters'
        ], features
