# -*- coding: utf-8 -*-


import unicodedata
import re

_non_word_matcher = re.compile("[^\w]+")


def canonicalize(text):
    asciiText = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore')
    return _non_word_matcher.sub('_', asciiText).lower()


def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False