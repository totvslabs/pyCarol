from datetime import datetime

def ranges(min_v, max_v, nb):
    if min_v == max_v:
        max_v += 1
    step = int((max_v - min_v) / nb) + 1
    step = list(range(min_v, max_v, step))
    if step[-1] != max_v:
        step.append(max_v)
    step = [[step[i], step[i + 1] - 1] for i in range(len(step) - 1)]
    step.append([max_v, None])
    return step


def delete_golden(carol, dm_name, now=None):
    """

    Delete Golden records.

    It Will delete all golden records of a given data model based on lastUpdate.

    :param carol: `pycarol.carol.Carol`
        Carol instance
    :param dm_name: `str`
        Data model name
    :param now: `str`
        Delete records where last update is less the `now`. Any date time ISO format is accepted.

    Usage:
    To delete:

    >>>from pycarol.utils.miscellaneous import delete_golden
    >>>from pycarol.auth.PwdAuth import PwdAuth
    >>>from pycarol.carol import Carol
    >>>login = Carol()
    >>>delete_golden(login, dm_name=my_dm)

    To delate based on a date.
    >>>delete_golden(login, dm_name=my_dm, now='2018-11-16')
    """

    from ..query import Query
    from ..filter import TYPE_FILTER, RANGE_FILTER, Filter

    if now is None:
        now = datetime.utcnow().isoformat(timespec='seconds')

    json_query = Filter.Builder() \
        .should(TYPE_FILTER(value=dm_name + "Golden")) \
        .should(TYPE_FILTER(value=dm_name + "Master")) \
        .must(RANGE_FILTER("mdmLastUpdated", [None, now])) \
        .build().to_json()

    try:
        Query(carol).delete(json_query)
    except:
        pass

    json_query = Filter.Builder() \
        .type(dm_name + "Rejected") \
        .must(RANGE_FILTER("mdmLastUpdated", [None, now])) \
        .build().to_json()

    try:
        Query(carol, index_type='STAGING').delete(json_query)
    except:
        pass
