import pytest
from random import randrange

from core.reviewee.assign import assign_reviewees


def get_sample(array):
    return array[:randrange(0, len(array) + 1)]


data_a_CTPJHL1H8 = ["UT4NDMZ5X", "UTETSCFPF", "UT3DE17S7", "UTECM5GQ4", "UT2403V7U", "UTETSGS7P", "UT2404E8J",
                    "UTH3YQC0N", "UT4NDJZ7T", "UTETSD57X", "UTEDTHNJG", "UTECM4B9S"]
data_b_CTPJHM0GJ = ["UT3DE56JF", "UT2408M18", "UTGP424P9", "UT4M77J01", "UTERZCK6U", "UT4NDQD7T", "UTGP42JGP",
                    "UTGP40807", "UT4NDT4PK", "UT4NDLWV7", "UT4NDT7U1", "UT240AL66"]
data_e_CU2C9MB96 = ["UT24095GA", "UTGP3SU87", "UTEDTJ7GC", "UTH3YUJKG", "UTCS7FLLD", "UT24037A6", "UT4NDMA3B",
                    "UT4M776U9"]
front_a_CU2CW93M3 = ["UT2403CLS", "UTEDTH16G", "UT4NDMU81", "UTESKVAN9", "UTET5UCV6", "UTEDTEJ2G", "UT240775G"]
front_b_CTPJJ07UJ = ["UT2409SF4", "UT3ERLVMZ", "UTET5P4N4", "UT4NDSJG1", "UTERZDAMN", "UT3DE3H0B", "UTGMXC9PZ"]
back_a_CU4882QUF = ["UTET5PHNC", "UTET5SLLQ", "UT4NDNW57", "UTGP3Q47R", "UT3DE2V5Z", "UTH3YQPEJ", "UTH2SARAS",
                    "UTH3YN2K0"]
back_b_CTS8D3FFT = ["UT3DE58MR", "UTET5TCHW", "UTGP421D5", "UTGP41UNB", "UTGP417GX", "UT3DE4MNX", "UTECM5SSG",
                    "UTEDTMDQU", "UTET61KQQ"]
android_CU2CW80KF = ["UTGP3S7RD", "UTETSGL69", "UT3DE5BLK", "UTH2S9MRC", "UTH3YSBQE"]


for _ in range(10000):
    data = {
        "data_a": {
            "reviewers": data_a_CTPJHL1H8,
            "reviewees": get_sample(data_a_CTPJHL1H8)
        },
        "data_b": {
            "reviewers": data_b_CTPJHM0GJ,
            "reviewees": get_sample(data_b_CTPJHM0GJ)
        },
        "data_e": {
            "reviewers": data_e_CU2C9MB96,
            "reviewees": get_sample(data_e_CU2C9MB96)
        },
        "front_a": {
            "reviewers": front_a_CU2CW93M3,
            "reviewees": get_sample(front_a_CU2CW93M3)
        },
        "front_b": {
            "reviewers": front_b_CTPJJ07UJ,
            "reviewees": get_sample(front_b_CTPJJ07UJ)
        },
        "back_a": {
            "reviewers": back_a_CU4882QUF,
            "reviewees": get_sample(back_a_CU4882QUF)
        },
        "back_b": {
            "reviewers": back_b_CTS8D3FFT,
            "reviewees": get_sample(back_b_CTS8D3FFT)
        },
        "android": {
            "reviewers": android_CU2CW80KF,
            "reviewees": get_sample(android_CU2CW80KF)
        }
    }

    results = assign_reviewees(data)

    for result in results:
        assert result["user_id"] not in result["reviewee_ids"]

        if len(result["reviewee_ids"]) > 1:
            assert result["reviewee_ids"][0] != result["reviewee_ids"][1]

