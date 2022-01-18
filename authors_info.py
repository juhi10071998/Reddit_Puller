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
url = "https://api.pushshift.io/reddit/search/submission"
subreddit_to_pull_submissions = "Immigration"
#output_file = '/Users/juhi/Downloads/output_reddit.txt'

def crawl_page(authors, start_date: str, end_date: str, last_page = None):
  """Crawl a page of results from a given subreddit.

  :param authors: list of users to query for, convert this to tuple to pass to the function call.
  :param last_page: The last downloaded page.
  :param start_time: after, beginning
  :param before: get reddits before this time.
  :return: A page or results.
  """
  ## here subreddit is actually the text file woth authors ka data.

  params = {"sort": "desc", "sort_type": "created_utc"}
  start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d")
  start_date_utc = int(start_date_datetime.replace(tzinfo=timezone.utc).timestamp())
  end_date_datetime = datetime.strptime(end_date, "%Y-%m-%d")
  end_date_utc = int(end_date_datetime.replace(tzinfo=timezone.utc).timestamp())

  users = tuple(authors)
  params["author"] = users
  ##end_time
  params["before"] = end_date_utc
  print(type(users))
  if last_page is not None:
    if len(last_page) > 0:
      # resume from where we left at the last page

      ##start time
      params["author"] = users
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
    #raise Exception("Server returned status code {}".format(results.status_code))
    results = requests.get(url, params)
  #print(results.json())
  return results.json()["data"]
import time

def crawl_subreddit(authors, start_date,end_date):
  """
  Crawl submissions from a subreddit.

  :param subreddit: The subreddit to crawl.
  :param max_submissions: The maximum number of submissions to download.

  :return: A list of submissions.
  """
  print(authors)
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
    last_page = crawl_page(authors, start_date,end_date,last_page)
    for reddit in last_page:
         author = reddit.get("author")
         author_fullname = reddit.get("author_fullname","")
         created_utc = reddit.get("created_utc","")
         domain = reddit.get("domain","")
         link = reddit.get("link","")
         id_ = reddit.get("id","")
         num_comments = reddit.get("num_comments","")
         is_original_content = reddit.get("is_original_content","")
         pwls = reddit.get("pwls","")
         score = reddit.get("score","")
         self_text = reddit.get("self_text","")
         subreddit = reddit.get("subreddit","")
         subreddit_id = reddit.get("subreddit_id","")
         title = reddit.get("title","")
         upvote_ratio = reddit.get("upvote_ratio",1.0)
         #upvote_ratio = 1.0
         print("upvote ratio IS SSSSS {}".format(upvote_ratio))
         selftext = reddit.get("selftext","")
         url = reddit.get("url","")
         selftext = selftext.replace("'","")
         selftext = selftext.replace('"',"")
         date = ""
         if created_utc != "":
             date = datetime.fromtimestamp(created_utc).date()
         try:
             print("inserting..")
             engine.execute('INSERT INTO reddit_immigrants(author,author_fullname,created_utc,date,domain,link,id,num_comments,is_original_content,pwls,score,selftext,subreddit,subreddit_id,title,upvote_ratio,url) VALUES("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")'.format(author,author_fullname,created_utc,date,domain,link,id_,num_comments,is_original_content,pwls,score,selftext,subreddit,subreddit_id,title,upvote_ratio,url))
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
    with open(subreddit_to_pull) as f:
        print("rradingg")
        lines = f.readlines()
        lines = [line.rstrip() for line in lines]

    lines = lines[1200:]
   # lines = lines.remove('deleted')
    print(lines[0])
    count = 0
    ## break in chunks of 100
    lines_list = [lines[i:i + 10] for i in range(0, len(lines), 10)]
    for ind_list in lines_list:

        cnt = crawl_subreddit(ind_list,start_date,end_date)
        print(cnt)
        count += cnt
    print("num issues: {}".format(count))
    print("done")
if __name__ == '__main__':
    main()
