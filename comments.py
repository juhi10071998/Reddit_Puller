import argparse
import time
import os
import json
import requests
from datetime import datetime
import sqlalchemy
import requests
import json
from datetime import timezone,datetime
#url = "https://api.pushshift.io/reddit/search/submission"
url = "https://api.pushshift.io/reddit/search/comment"
subreddit_to_pull_submissions = "immigration"
#output_file = '/Users/juhi/Downloads/output_reddit.txt'

def crawl_page(subreddit: str, start_date: str, end_date: str, last_page = None):
  """Crawl a page of results from a given subreddit.

  :param subreddit: The subreddit to crawl.
  :param last_page: The last downloaded page.
  :param start_time: after, beginning
  :param before: get reddits before this time.
  :return: A page or results.
  """
  params = {"subreddit": subreddit, "sort": "desc", "sort_type": "created_utc"}
  start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d")
  start_date_utc = int(start_date_datetime.replace(tzinfo=timezone.utc).timestamp())
  end_date_datetime = datetime.strptime(end_date, "%Y-%m-%d")
  end_date_utc = int(end_date_datetime.replace(tzinfo=timezone.utc).timestamp())
  #params["author"] = "Zombieblunt"
  ##end_time
  params["before"] = end_date_utc #5 november

  if last_page is not None:
    if len(last_page) > 0:
      # resume from where we left at the last page

      ##start time
      #params["author"] = "Zombieblunt"
      params["after"] = start_date_utc # 5 nov
      params["before"] = last_page[-1]["created_utc"]
#      print(params["before"])
    else:
      # the last page was empty, we are past the last page
      return []
  results = requests.get(url, params)
  if not results.ok:
    # something wrong happend
    ''' If I get some error like Bad gateway, what you think would be best, to call the crawl_page back? I got a 502 http, but how do I put a limit until which it should crawl over again?
    results = requests.get(url, params)'''
    raise Exception("Server returned status code {}".format(results.status_code))
  print(results.json())
  return results.json()["data"]

import time

def crawl_subreddit(subreddit, start_date,end_date):
  """
  Crawl submissions from a subreddit.
  :param subreddit: The subreddit to crawl.
  :param max_submissions: The maximum number of submissions to download.

  :return: A list of submissions.
  """
  count =0
  submissions = []
  db = sqlalchemy.engine.url.URL(drivername='mysql',
                            host='127.0.0.1',
                            database='juhimittal',
                            query={'read_default_file': '~/.my.cnf', 'charset':'utf8mb4'})
  engine = sqlalchemy.create_engine(db)

  connection = engine.connect()
  last_page = None
  while last_page != []:
    last_page = crawl_page(subreddit, start_date,end_date,last_page)
    for reddit in last_page:
         author = reddit.get("author")
         author_fullname = reddit.get("author_fullname","")
         author_flair_text = reddit.get("author_flair_text","")
         created_utc = reddit.get("created_utc","")
         is_submitter = reddit.get("is_submitter","")
         link_id = reddit.get("link_id","")
         message = reddit.get("body","")
         message_id = reddit.get("id")
         parent_id = reddit.get("parent_id","")
         permalink = reddit.get("permalink","")
         score = reddit.get("score","")
         subreddit = reddit.get("subreddit","")
         subreddit_id = reddit.get("subreddit_id","")
         message = message.replace("'","")
         message = message.replace('"',"")
         date = ""
         if created_utc != "":
             date = datetime.fromtimestamp(created_utc).date()
         try:
             print("inserting..")
             engine.execute('INSERT INTO comments_immigration(author,author_fullname,author_flair_text,created_utc,date,is_submitter,link_id, message, message_id, parent_id, permalink, score, subreddit, subreddit_id) VALUES("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")'.format(author,author_fullname,author_flair_text,created_utc,date,is_submitter,link_id,message,message_id,parent_id,permalink,score,subreddit,subreddit_id))
         except Exception as e:
             print("issue...")
             count  =count+1
             print(e)
    time.sleep(3)
  return count
def main():
    parser = argparse.ArgumentParser(description='Downloaded reddits')
    parser.add_argument(
        '--start_date', help='pull reddist after this')

    parser.add_argument(
        '--end_date', help ='pull before this')


    parser.add_argument(
        '--subreddit', help = 'subreddit to pull for')


    args = parser.parse_args()
    subreddit_to_pull = args.subreddit
    start_date = args.start_date
    end_date = args.end_date
    cnt  = crawl_subreddit(subreddit_to_pull,start_date,end_date)
    print("num issues: {}".format(cnt))
    print("done")
if __name__ == '__main__':
    main()
