from random import shuffle
from typing import List


def get_reviewees(candidates: List[str], reviewers: List[str]) -> List[str]:
    multiple = (len(reviewers) * 2 // len(candidates)) + 1
    reviewees = []

    for _ in range(multiple):
        members = candidates[:]
        shuffle(members)
        reviewees += members

    last_reviewer = reviewers[-1]
    i = len(reviewees) - 2

    if reviewees[i] is last_reviewer:
        n = len(candidates)
        reviewees[i], reviewees[-n] = reviewees[-n], reviewees[i]

    if reviewees[i + 1] is last_reviewer:
        n = len(candidates)
        reviewees[i + 1], reviewees[-n + 1] = reviewees[-n + 1], reviewees[i + 1]

    return reviewees


def swap_for_preventing_review_myself(reviewees: List[str], assignees: [str, str], cursor: int):
    duplicate = assignees[cursor]
    index = [index for index, reviewee in enumerate(reviewees) if reviewee is not duplicate][0]

    assignees[cursor] = reviewees.pop(index)
    reviewees.insert(0, duplicate)


def swap_for_same_reviewee(reviewees: List[str], assignees: [str, str], reviewer: str):
    duplicate = assignees[0]
    index = [index for index, reviewee in enumerate(reviewees) if reviewee is not duplicate and reviewee is not reviewer][0]

    assignees[0] = reviewees.pop(index)
    reviewees.insert(0, duplicate)


def assign_reviewees(teams):
    assignments = []

    for team_id in teams.keys():
        team = teams[team_id]
        reviewers = team["reviewers"]

        if len(team["reviewees"]) <= 2:
            for reviewer in team["reviewers"]:
                assignments.append({"id": reviewer, "reviewees": [reviewee for reviewee in team["reviewees"] if reviewee is not reviewer]})
        else:
            reviewees = get_reviewees(team["reviewees"], reviewers)

            for reviewer in reviewers:
                assignees = reviewees[:2]
                draft_reviewees = reviewees[2:]

                while reviewer in assignees:
                    swap_for_preventing_review_myself(draft_reviewees, assignees, assignees.index(reviewer))

                while len(assignees) >= 2 and assignees[0] is assignees[1]:
                    swap_for_same_reviewee(draft_reviewees, assignees, reviewer)

                assignments.append({"user_id": reviewer, "reviewee_ids": assignees})
                reviewees = draft_reviewees

    return assignments


def ready_for_sending_bq(deadline: str, teams):
    assignments = assign_reviewees(teams)

    for assignment in assignments:
        assignment.update({"deadline": deadline})

    return assignments

