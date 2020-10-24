from random import randrange, shuffle
from math import ceil
from operator import itemgetter


def get_sample(array):
    return array[:randrange(0, len(array) + 1)]


android_CU2CW80KF = ["UTGP3S7RD", "UTETSGL69", "UT3DE5BLK", "UTH2S9MRC", "UTH3YSBQE"]

data = {
    "android_CU2CW80KF": {
        "reviewees": get_sample(android_CU2CW80KF),
        "reviewers": {
            "UTGP3S7RD": {
                "UTETSGL69": 0,
                "UT3DE5BLK": 0,
                "UTH2S9MRC": 0,
                "UTH3YSBQE": 0
            },
            "UTETSGL69": {
                "UTGP3S7RD": 0,
                "UT3DE5BLK": 0,
                "UTH2S9MRC": 0,
                "UTH3YSBQE": 0
            },
            "UT3DE5BLK": {
                "UTGP3S7RD": 0,
                "UTETSGL69": 0,
                "UTH2S9MRC": 0,
                "UTH3YSBQE": 0,
            },
            "UTH2S9MRC": {
                "UTGP3S7RD": 0,
                "UTETSGL69": 0,
                "UT3DE5BLK": 0,
                "UTH3YSBQE": 0
            },
            "UTH3YSBQE": {
                "UTGP3S7RD": 0,
                "UTETSGL69": 0,
                "UT3DE5BLK": 0,
                "UTH2S9MRC": 0
            }
        }
    }
}

for _ in range(10000):


    for assignment in assignments:
        assert assignment["user_id"] not in assignment["reviewee_ids"]

        if len(assignment["reviewee_ids"]) > 1:
            assert assignment["reviewee_ids"][0] != assignment["reviewee_ids"][1]

