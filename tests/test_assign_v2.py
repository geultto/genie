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
    assignments = []

    for team in data.values():
        reviewees = []
        multiple = ceil(len(team["reviewers"]) * 2 / len(team["reviewees"]))

        for _ in range(multiple):
            members = team["reviewees"][:]
            shuffle(members)
            reviewees += members

        reviewers = list(team["reviewers"].keys())
        shuffle(reviewers)

        for reviewer in reviewers:
            stack = []

            candidates = [(reviewee, count) for reviewee, count in team["reviewers"][reviewer].items()]

            candidates.sort(key=itemgetter(1))

            for candidate, _ in candidates:
                try:
                    index = reviewees.index(candidate)
                    target = reviewees.pop(index)
                    stack.append(target)
                except ValueError:
                    pass

                if len(stack) == 2:
                    break

            assignments.append({"user_id": reviewer, "reviewee_ids": stack})


    for assignment in assignments:
        assert assignment["user_id"] not in assignment["reviewee_ids"]

        if len(assignment["reviewee_ids"]) > 1:
            assert assignment["reviewee_ids"][0] != assignment["reviewee_ids"][1]

