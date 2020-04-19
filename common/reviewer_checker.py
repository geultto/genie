import os
import pandas as pd
from google.oauth2 import service_account

# 빅쿼리 인증 
_credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

# load tbls
def read_tables():
  user_mapping_df = pd.read_gbq(
      query='''select user_id, reviewee_ids
              from geultto_4th_staging.review_mapping_raw_20200414_030800''', 
      credentials=_credentials)

  feedbacks_df = pd.read_gbq(
      query=('select * from geultto_4th_prod.submit_post as feedback where feedback.round = 4'),
      credentials=_credentials)

  users_df = pd.read_gbq(
    query=('select * from geultto_4th_staging.user'),
    credentials=_credentials
  )
  return user_mapping_df, feedbacks_df, users_df

# set pass & null list
def create_list_to_excepted(feedbacks_df):
  pass_list = feedbacks_df[feedbacks_df.reaction == 'pass'].reset_index(drop=True)
  pass_list['feedback_count'] = 'pass'
  null_list = feedbacks_df[feedbacks_df['reaction'].isnull()].reset_index(drop=True)
  null_list['feedback_count'] = None
  return pass_list, null_list

def filter_df(user_mapping_df, pass_list, null_list):
  # except pass and null from reviewer mapping table
  user_mapping_df = user_mapping_df.query('user_id not in @pass_list.user_id.tolist() and user_id not in @null_list.user_id.tolist()').reset_index(drop=True)

  # str -> list
  user_mapping_df['reviewee_ids'] = [x.replace('[','').replace(']','').replace("'",'').split(', ') for x in user_mapping_df['reviewee_ids']]
  return user_mapping_df

# checking feedback
def check_feedback(user_mapping_df, feedbacks_df):
  checked_list = []
  for reviewer_id in user_mapping_df['user_id']:
    writer_ids = feedbacks_df[feedbacks_df.user_id == reviewer_id].drop_duplicates(['parent_user_id']).parent_user_id
    reviewee_list = user_mapping_df[user_mapping_df.user_id == reviewer_id].reset_index(drop=True).reviewee_ids

    feedback_cnt = writer_ids.isin(reviewee_list[0]).sum()
    checked_list.append({"user_id": reviewer_id, "feedback_count": feedback_cnt})

  print(checked_list)
  return checked_list

# to_df & bq
def send_bq(checked_list, pass_list, null_list, users_df):
  review_checked_df = pd.DataFrame(data=checked_list)
  review_checked_df = pd.concat([review_checked_df, pass_list[['user_id', 'feedback_count']], null_list[['user_id', 'feedback_count']]], axis=0)
  review_checked_df = pd.merge(review_checked_df, users_df, on='user_id')[['user_id', 'user_name', 'channel_name', 'feedback_count']]
  review_checked_df.to_gbq(f'geultto_4th_prod.check_feedback', project_id='geultto', if_exists='replace', credentials=_credentials)

if __name__ == '__main__':
  user_mapping_df, feedbacks_df, users_df = read_tables()
  pass_list, null_list = create_list_to_excepted(feedbacks_df)
  user_mapping_df = filter_df(user_mapping_df, pass_list, null_list)
  checked_list = check_feedback(user_mapping_df, feedbacks_df)
  send_bq(checked_list, pass_list, null_list, users_df)
