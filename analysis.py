import pandas as pd
import networkx as nx
import math
import time
from itertools import combinations
from pymongo import MongoClient

mongoclient = MongoClient()


# 读入传销话题所有问题数据
def read_mongodb_df(db, collection):
    cursor = mongoclient[db][collection].find()
    df = pd.DataFrame(list(cursor))
    return df


def df_topics_tonodes(df):
    """
    将问题的话题列表展开，如[[{},{}],[{},{}]]-->[{},{},{}]；
    根据话题出现次数作为话题权重，生成node数据
    """
    topics = pd.DataFrame([item for sublist in df["topics"] for item in sublist])
    topics_freq = topics.groupby("topic_name").size().reset_index(name="size").sort_values("size", ascending=False)
    topics_freq["size_log2"] = [round(math.log2(x), 2) for x in topics_freq["size"]]
    return topics_freq


def topis_mark_init():
    """
    设置初始化的话题类型字典；默认未分类类型为8:"未分类"
    """
    df = read_mongodb_df(DB, Collections1).drop_duplicates("question_id", keep="last").reset_index(drop=True)
    topics_freq = df_topics_tonodes(df)
    topics_freq.to_csv(Topics_Freq_Url, index=None)
    topics_freq["类型"] = 8
    topics_mark = topics_freq[["topic_name","类型"]].reset_index(drop=True)
    topics_mark.to_csv(Topics_notMarked_Url,index = None)
    return topics_mark

def label_toclass(topics_freq):
    """
    根据话题类型字典，将获得话题进行类型标记
    """
    topics_freq["class"] = topics_freq["topic_name"].map(topics_mark_dict)
    topics_freq = topics_freq.dropna().rename(columns={"topic_name": "label"}).sort_values("class").reset_index(
        drop=True)
    topics_freq.index.name = "node_id"
    return topics_freq


##从Topics数据结构生成问题话题的两两组合数据，作为edge数据
def df_topics_tolinks(df):
    question_topic = []
    for i in range(0, len(df)):
        question_topic.append([x["topic_name"] for x in df["topics"][i]])
    links = []
    for i in question_topic:
        temp = list(combinations(i, 2))
        links = temp + links
    links = pd.DataFrame(links, columns=("Source", "Target"))
    return links


'''回答数按时间和话题类型的可视化；针对问题的话题，将该问题下的回答针对话题进行了展开。
认为回答者回答该问题，对该问题的所有相关话题都感兴趣，权重一致。'''


##根据分类字典，将话题转换为类型名称，并将每个话题展开，问题id,[1,3,4]-->(id,1),(id,3),(id,4)分开；
def topic_unfold(answers_df):
    # 话题类型匹配
    s = []
    for i in range(0, len(answers_df)):
        temp = pd.Series([x["topic_name"] for x in answers_df["topics"][i]]).map(topics_mark_dict).dropna()
        s.append(list(temp))
    answers_df["class"] = s
    # 话题展开
    answers_topic_class = []
    for x in range(0, len(answers_df)):
        for i in range(0, len(answers_df["class"][x])):
            temp = {}
            temp["anthor"] = answers_df["author"][x]
            temp["created_time"] = answers_df["created_time"][x]
            temp["voteup_count"] = answers_df["voteup_count"][x]
            temp["question_class"] = answers_df["class"][x][i]
            answers_topic_class.append(temp)
    answers_topic_class = pd.DataFrame(answers_topic_class).drop_duplicates()
    return answers_topic_class


##时间处理
def trans_time(timestamp):
    time_local = time.localtime(timestamp)
    dt = time.strftime("%Y/%m/%d", time_local)
    return dt


# 作图需要类型名称，分类id转名称，
def class_tolabel(answers_topic_class):
    answers_topic_class["question_class"] = answers_topic_class["question_class"].map(class_mark_dict)
    answer_all = answers_topic_class.groupby(["question_class", "created_time"]).size().reset_index(
        name="count").sort_values(by=["question_class", "created_time"])
    return answer_all


# 手工标记数据类别，将话题分类，类别为0-7的分类变量；0-7未指定类型；


def gexf_output():
    global DB, Collections1, Gexf_File_Url
    '''
    生成话题关系图，及简单类型分布分析
    '''
    # 读取数据
    df = read_mongodb_df(DB, Collections1).drop_duplicates("question_id", keep="last").reset_index(drop=True)
    # 话题展开，频数统计
    topics_freq = df_topics_tonodes(df)
    # 根据话题类型标记字典，标记话题类型
    topics_freq = label_toclass(topics_freq)
    # print(topics_freq,type(topics_freq))
    # pd.DataFrame.to_csv("output/nodes.csv",topics_freq)

    # 问题的话题是生成组合，构造边数据
    links = df_topics_tolinks(df)
    # 根据筛选的分类目录整理边连接关系，去除不在分类的话题
    links = links[(links.Source.isin(topics_freq["label"])) & (links.Target.isin(topics_freq["label"]))]
    links = links.groupby(["Source", "Target"]).size().reset_index(name="weight").sort_values("weight", ascending=False)
    # links的Source和Target转成id,label转成id
    label_id = dict(zip(topics_freq["label"], topics_freq.index))
    links["Source_id"] = links["Source"].map(label_id)
    links["Target_id"] = links["Target"].map(label_id)
    # print(links,type(links))
    # pd.DataFrame.to_csv("output/links.csv",links)
    with open(Gexf_File_Url, "w", encoding="utf-8") as  f:
        nodes_txt = ""
        for i in range(0, len(topics_freq)):
            str0 = "{}".format(topics_freq.index[i])
            str1 = "{}".format(topics_freq.iloc[i]["label"])
            str2 = "{}".format(int(topics_freq.iloc[i]["class"]))
            str3 = "{}".format(topics_freq.iloc[i]["size_log2"])
            p = "<node id= " + "\"" + str0 + "\"" + " " + "label=" + "\"" + str1 + "\"" + " >\n" \
                + "<attvalues>" + '<attvalue for ="modularity_class" ' + " " + "value = " + "\"" + str2 + "\"" + "></attvalue ></attvalues>\n" \
                + "<viz:size value =" + "\"" + str3 + "\"" + "> </viz:size>\n" + "</node> \n"
            nodes_txt = nodes_txt + p
        links_txt = ""
        for i in range(0, len(links)):
            edge_id = str(i)
            str1 = "{}".format(links.iloc[i]["Source_id"])
            str2 = "{}".format(links.iloc[i]["Target_id"])
            str3 = "{}".format(links.iloc[i]["weight"])
            p = '<edge id= ' + "\"" + edge_id + "\"" + " " + "source=" + "\"" + str1 + "\"" + " " + "target =" + "\"" + str2 + "\"" + " " + "weight = " + "\"" + str3 + "\"" + "></edge >\n"
            links_txt = links_txt + p
        head_txt = '''<?xml version="1.0" encoding="UTF-8"?><gexf xmlns="http://www.gexf.net/1.2draft" version="1.2" xmlns:viz="http://www.gexf.net/1.2draft/viz" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd">
    <graph defaultedgetype="undirected" mode="static">
      <attributes class="node" mode="static">
        <attribute id="modularity_class" title="Modularity Class" type="integer"></attribute>
      </attributes>
      <nodes>'''
        all = head_txt + nodes_txt + "</nodes>\n" + "<edges>" + links_txt + "</edges>\n</graph>\n</gexf>"
        f.write(all)
    f.close()


def answertype_bytime_output():
    global DB, Collections1, Collections2, Gexf_File_Url, Time_File_Url
    '''
    以下为各类型回答按时间累计分析 
    '''
    # 话题数据
    topics_df = read_mongodb_df(DB, Collections1)[["question_id", "topics"]].drop_duplicates("question_id",keep="last").reset_index(drop=True)  ##获取数据有重复，要去重
    # 回答数据
    answers_df = read_mongodb_df(DB, Collections2)[["answer_id", "author", "created_time", "question_id", "voteup_count"]].drop_duplicates("answer_id",keep="last").reset_index(drop=True)  ##获取数据有重复，要去重
    ##数据合并
    answers_df = pd.merge(topics_df, answers_df, how="right", on="question_id")
    # 话题展开
    answers_topic_class = topic_unfold(answers_df)
    # 时间戳转换
    answers_topic_class["created_time"] = [trans_time(x) for x in answers_topic_class["created_time"]]
    # 类型id转名称,为方便时间分组，对时间进行处理
    answer_all = class_tolabel(answers_topic_class)
    answer_all["created_time"] = pd.to_datetime(answer_all["created_time"])
    answer_all = answer_all.set_index("created_time")
    ##按月汇总
    answer_bymonth = answer_all.groupby(["question_class", pd.TimeGrouper('M')])["count"].sum().fillna(0).reset_index()
    input_data = answer_bymonth[answer_bymonth["created_time"] > "2015"]
    with open(Time_File_Url, "w", encoding="utf-8") as  f:
        s = ""
        for i in range(0, len(input_data)):
            str1 = "{}".format(str(input_data.iloc[i]["created_time"]))
            str2 = "{}".format(input_data.iloc[i]["count"])
            str3 = input_data.iloc[i]["question_class"]
            p = "[" + '\"' + str1 + '\"' + "," + str2 + "," + '\"' + str3 + '\"' + "]" + ",\n"
            s = s + p
        f.write(s)
        f.close()



DB = "Tencent"
Collections1 = "Topics"
Collections2 = "Answers"
Topics_Marked_Url = "Topics_Marks/Havemarked/{}.csv".format(DB)
Topics_notMarked_Url = "Topics_Marks/Notmark/{}.csv".format(DB)
Topics_Freq_Url = "Topics_Marks/Topic_freq/{}.csv".format(DB)
Gexf_File_Url = "../caixin_works/data/{}.gexf".format(DB)
Time_File_Url = "output/{}.txt".format(DB)

class_mark_dict = {"class": [0, 1, 2, 3, 4, 5, 6, 7, 8],
                   "name": ["企业/品牌", "形式/类型", "观点/认知", "地理区域", "相关人物", "机构领域", "人群需求", "角色职业", "未分类"]}
class_mark_dict = dict(zip(class_mark_dict["class"], class_mark_dict["name"]))

# topics_mark = pd.read_csv(Topics_Marked_Url, encoding="gb2312",engine='python')  ##人工进行类型标注
topics_mark = topis_mark_init() ##所有话题类型字典
topics_mark = topics_mark[1:300]
topics_mark_dict = dict(zip(topics_mark["topic_name"], topics_mark["类型"]))


def main():
    topis_mark_init()
    gexf_output()
    # answertype_bytime_output()


if __name__ == "__main__":
    main()
