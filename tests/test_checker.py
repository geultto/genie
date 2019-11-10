import pytest
from checker import self_reaction_check, check_deadline


@pytest.fixture(scope='session', autouse=True)
def submit_message():
    message = {'user': 'UKHNECPGV',
               'ts': '1563016554.090200',
               'text': 'GDG WebTech 에 다녀오고 나서 그에 대한 참관기를 작성해 보았습니다',
               'attachments': [{'service_name': 'Medium',
                                'title': '2019 GDG WebTech 참관기 - Medium',
                                'title_link': 'https://medium.com/@pks2974/2019-gdg-webtech-%EC%B0%B8%EA%B4%80%EA%B8%B0-9bcd96b12fd',
                                'text': 'Google Developers Groups 에서 주최한 WebTech 를 다녀왔고 이를 정리해보려고 한다.',
                                'fallback': 'Medium: 2019 GDG WebTech 참관기 - Medium',
                                'ts': 1563016481,
                                'from_url': 'https://medium.com/@pks2974/2019-gdg-webtech-%EC%B0%B8%EA%B4%80%EA%B8%B0-9bcd96b12fd',
                                'service_icon': 'https://cdn-images-1.medium.com/fit/c/152/152/1*8I-HPL0bfoIzGied-dzOvA.png',
                                'id': 1,
                                'original_url': 'https://medium.com/@pks2974/2019-gdg-webtech-%EC%B0%B8%EA%B4%80%EA%B8%B0-9bcd96b12fd'}],
               'reactions': [{'name': '+1',
                              'users': ['UKGNN98VB', 'UKK27JVHC', 'UKJU9HZGW', 'UKGQ3BE5N', 'UKH6CHHG8', 'UKHNECPGV'],
                              'count': 6},
                             {'name': 'submit',
                              'users': ['UKHNECPGV', 'UKK27JVHC'],
                              'count': 2},
                             ]}
    return message


@pytest.fixture(scope='session', autouse=True)
def pass_message():
    message = {'user': 'UKK27JVHC',
               'ts': '1563016554.090200',
               'text': '패스 사용합니다',
               'reactions': [{'name': 'pass',
                              'users': ['UKK27JVHC'],
                              'count': 6}]}
    return message


def test_self_reaction_check(submit_message, pass_message):
    assert self_reaction_check('submit', submit_message)
    assert not self_reaction_check('submit', pass_message)
    assert self_reaction_check('pass', pass_message)
    assert not self_reaction_check('pass', submit_message)


def test_check_deadline():
    assert check_deadline('2019-07-22', '2019-07-21 23:21:24', 'submit')
    assert not check_deadline('2019-07-22', '2019-07-23 23:21:24', 'submit')
    assert check_deadline('2019-07-22', '2019-07-21 23:21:24', 'pass')
    assert not check_deadline('2019-07-22', '2019-07-23 23:21:24', 'pass')
