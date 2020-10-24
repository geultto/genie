from random import shuffle
from math import ceil
from operator import itemgetter
from pandas.io.gbq import read_gbq


def read_sql(file_name):
    with open(file_name, 'r') as file:
        s = file.read()
    return s


def assign_reviewees(data):
    assignments = []

    for team in data.values():
        reviewers = list(team["reviewers"].keys())
        multiple = ceil(len(reviewers) * 2 / len(team["reviewees"]))
        reviewees = sum([team["reviewees"][:] for _ in range(multiple)], [])

        shuffle(reviewers)

        for reviewer in reviewers:
            stack = []

            candidates = [(reviewee, count) for reviewee, count in team["reviewers"][reviewer].items()]
            shuffle(candidates)
            candidates.sort(key=itemgetter(1))

            print(candidates)
            print(reviewees)

            for candidate, _ in candidates:
                try:
                    reviewees.remove(candidate)
                    stack.append(candidate)
                except ValueError:
                    pass

                if len(stack) == 2:
                    break

            assignments.append({"user_id": reviewer, "reviewee_ids": stack})

    return assignments


if __name__ == '__main__':
    df = read_gbq(query=read_sql('query_v2_review_count.sql'), project_id='geultto')
    data = {}

    for _, row in df.iterrows():
        channel_id = row["channel_id"]
        reviewer = row["reviewer"]
        reviewee = row["reviewee"]
        cnt = row["cnt"]

        if channel_id not in data:
            data[channel_id] = {
                "reviewers": {
                    reviewer: {
                        reviewee: cnt
                    }
                }
            }
        elif reviewer not in data[channel_id]["reviewers"]:
            data[channel_id]["reviewers"].update({reviewer: {reviewee: cnt}})
        else:
            data[channel_id]["reviewers"][reviewer].update({reviewee: cnt})

    date_kr_due = '2020-04-12'
    df2 = read_gbq(query=read_sql('query_v2_reviewees.sql').format(date_kr_due=date_kr_due), project_id='geultto')

    for _, row in df2.iterrows():
        channel_id = row["channel_id"]
        reviewees = row["reviewees"]

        data[channel_id]["reviewees"] = reviewees

    assignments = assign_reviewees(data)

    for assignment in assignments:
        print(assignment)
