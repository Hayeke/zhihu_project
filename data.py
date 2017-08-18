from multiprocessing import  Pool
import os
from zhihu_oauth import ZhihuClient
from zhihu_oauth.exception import NeedCaptchaException,GetDataErrorException
from zhihu_oauth.helpers import shield, SHIELD_ACTION
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pandas import Series,DataFrame
import pandas as pd

#登录初始化
def login_set(email = "909521500@qq.com",key = "1990feixiang"):
    TOKEN_FILE = 'token.pkl'
    global zhclient,topics
    if os.path.isfile(TOKEN_FILE):
        zhclient.load_token(TOKEN_FILE)
    else:
        zhclient.login_in_terminal(email,key)
        zhclient.save_token(TOKEN_FILE)

#获得某话题热门问题基本信息及话题信息
def topic_hotque(topic_id):
    global topic_collection
    topic = zhclient.topic(topic_id)
    topic_question = {}
    for best_answer in shield(topic.best_answers):
        topic_question["question_id"] = best_answer.question.id
        topic_question["question_title"] = best_answer.question.title
        topic_question["follower_count"] = best_answer.question.follower_count
        topic_question["created_time"] = best_answer.question.created_time
        topic_question["answer_count"] = best_answer.question.answer_count
        topic_question["topics"] = []
        question = zhclient.question(best_answer.question.id)
        for topic in question.topics:
            dic = {"topic_id":topic.id,"topic_name":topic.name,"topic_follower_count":topic.follower_count }
            topic_question["topics"].append(dic)
        topic_collection.insert_one(topic_question.copy())

#获得关注某问题的用户信息
def question_follower(question_id):
    global zhclient,topics,follower_collection
    question = zhclient.question(question_id)
    followers = {}
    for follower in  shield(question.followers):
        try:
            followers["name"] = follower.name
            followers["id"] = follower.id
            followers["follower_count"] = follower.follower_count
            #地址
            followers["location"] = "未填写"
            if follower.locations is not None:
                for location in follower.locations:
                    if location.name:
                        followers["location"] = location.name
                    else:
                        followers["location"] = "未填写"
                    break
            #行业
            if follower.business:
                followers["business"] = follower.business.name
            else:
                followers["business"] = "未填写"
            #教育
            followers["school"] = "未填写"
            if follower.educations is not None:
                for education in follower.educations:
                    if "school" in education:
                        followers["school"] = education.school.name
                    else:
                        followers["school"] = "未填写"
                    break
            follower_collection.insert_one(followers.copy())
        except GetDataErrorException as e:
            print('Get data error', e.reason)

#获得某问题下的所有答案信息
def question_answer(question_id):
    question = zhclient.question(question_id)
    global answer_collection
    answers = {}
    for answer in shield(question.answers):
        try:
            answers["question_id"] = answer.question.id
            answers["answer_id"] = answer.id
            answers["author"] = answer.author.name
            answers["voteup_count"] = answer.voteup_count
            answers["created_time"] = answer.created_time
            answer_collection.insert_one(answers.copy())
            print(answers)
        except GetDataErrorException as e:
            print('Get data error', e.reason)

#获得某话题下的所有回答信息，Topic_Name，MongoDB中话题表名称
def Answers(Topic_Name = "Topics"):
    if not isinstance(Topic_Name, (str)):
        raise TypeError('bad operand type')
    global topics
    # cursor = topics[Topic_Name].find({"question_id":23147234})
    cursor = topics[Topic_Name].find(no_cursor_timeout = True)
    for document in cursor:
        question_id = document["question_id"]
        question_answer(question_id)
        # print(document["question_id"])
        # break
    cursor.close()


zhclient = ZhihuClient()
mongoclient = MongoClient()

#话题数据库命名
topics = mongoclient["传销"]
#话题-回答信息表
answer_collection = topics["Answers"]
#话题-精华问答信息表
topic_collection = topics["Topics"]
#话题-关注用户信息表
follower_collection = topics["Followers"]

#高阶函数，获取话题id下所有问题，和回答信息
def Topic_all(Topic_id):
    topic_hotque(Topic_id)
    Answers()

##

def main():
    # Topic_all(19596997)#
    Answers("Topics")

if __name__=="__main__":
    login_set()
    main()

