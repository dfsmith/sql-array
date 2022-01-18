#!/usr/bin/env python3
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
    db = SQLArray("db1")["sqlarray"]
    db[time.time()] = time.ctime()

    db = SQLArray("db2")
    db["secondary"]["really"] = "phreooooow!"
    db["another"]["yokeydokey"] = "uh-huh-huh"

    db = SQLArray("db2", convert=json.dumps, unconvert=json.loads)
    array = db['sqlarray']
    d = {}
    d['one'] = "1"
    d['two'] = "2"
    array['object'] = d

    db = SQLArray("db3")
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

    db = SQLArray("db2", convert=json.dumps, unconvert=json.loads)
    d = db['sqlarray']['object']
    if d['one'] != "1" or d['two'] != "2":
        raise ValueError(f"{db}: incorrect object: {d}")

    db = SQLArray("db3")
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
print("Examine DB contents with ./sqlarray/sqlarray.py db[x]_sa.sqlite")
