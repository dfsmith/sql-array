#!/usr/bin/env python3
"""
Basic functionality check.
"""
from sqlarray import SQLArray
import json
import time
import sys


def wordstream(file):
    with open(file) as f:
        for line in f.readlines():
            for word in [x for x in line.split(' ') if x]:
                yield word.strip()


def populate():
    db = SQLArray("db1")
    array = db["sqlarray"]
    array[time.time()] = time.ctime()
    try:
        db['Robert"); DROP TABLE *']["student"] = "Little Bobby Tables"
    except KeyError as err:
        if "invalid table name" in err.args[0]:
            pass
        else:
            raise err
    else:
        raise KeyError("bad table name permitted")

    db = SQLArray("db2")
    db["another"]["elvis"] = "uh-huh-huh"
    db["another"]["jackson"] = "woo-hoo"
    db["another"]["glass"] = "zzz"
    db["secondary"]["really"] = "phreooooow!"

    db = SQLArray("db3", convert=json.dumps, unconvert=json.loads)
    array = db['sqlarray']
    d = {}
    d['one'] = "1"
    d['two'] = "2"
    array['object'] = d

    db = SQLArray("db4")
    words = SQLArray.Table(db, "words", key_type=int)
    index = SQLArray.Table(db, "counter_index", value_type=int)
    try:
        n = index['count']
    except:
        n = 0
    for word in wordstream(sys.argv[0]):
        words[n] = word
        n += 1
    index['count'] = n


def check():
    array = SQLArray("db1")['sqlarray']
    print(f"Test creation time:")
    for t in array:
        print(f"{t}: {array[t]}")

    db = SQLArray("db2")
    if not db["secondary"]["really"] == "phreooooow!":
        raise ValueError(f"{db}: secondary mismatch")
    array = db['another']
    found = 0
    ok = False
    for key in array.search('huh'):
        if key == 'elvis':
            ok = True
        found += 1
    if not (ok and found == 1):
        raise ValueError(f"{db}: search failed found={found} ok={ok}")

    db = SQLArray("db3", convert=json.dumps, unconvert=json.loads)
    d = db['sqlarray']['object']
    if d['one'] != "1" or d['two'] != "2":
        raise ValueError(f"{db}: incorrect object: {d}")

    db = SQLArray("db4")
    words = db['words']
    index = db['counter_index']
    count = index['count']
    n = 0
    while True:
        for word in wordstream(sys.argv[0]):
            if not words[n] == word:
                raise ValueError(
                    f"{db}: error words[{n}]=='{words[n]}' but should be '{word}'")
            n += 1
        if n == count:
            break
    return


populate()
check()
print("Examine DB contents then clean up with:\n"
      "  ./sqlarray/sqlarray.py db*_sa.sqlite\n"
      "  rm db*_sa.sqlite"
      )
