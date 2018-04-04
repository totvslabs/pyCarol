import requests
import json
from ..queriesCarol import queryCarol

def scrub(obj, bad):
    '''
    remove recursively a list of keys in a dictionary.  
    :param obj: dictionary
    :param bad: list of keys
    :return: empty
    '''
    if isinstance(obj, dict):
        iter_k = list(obj.keys())
        for kk in reversed(range(len(iter_k))):
            k = iter_k[kk]
            if k in bad:
                del obj[k]
            else:
                scrub(obj[k], bad)
    elif isinstance(obj, list):
        for i in reversed(range(len(obj))):
            if obj[i] in bad:
                del obj[i]
            else:
                scrub(obj[i], bad)

    else:
        # neither a dict nor a list, do nothing
        pass



