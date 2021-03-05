import pymongo
from datetime import datetime,timedelta
import os

__author__ = "Yan Huang"
__copyright__ = "Copyright 2020, GQ Group at UT Health"
__credits__ = ["Yan Huang"]
__license__ = ""
__version__ = "0.0.1"
__maintainer__ = "Yan Huang"
__email__ = "yan.huang@uth.tmc.edu"
__status__ = "development"

class QueryClient:
  def __init__(self,url,db_name):
    self.__mongo_client = pymongo.MongoClient(url)
    self.db = self.__mongo_client[db_name]


  def get_value_set(self,col_name=None,concept=None,value=None,freq=False):
    if not value:
      value = {"$exists":True}
    stmt = {"value":value}
    if col_name:
      stmt["col_name"] = col_name
    if concept:
      stmt["concept"] = concept
    docs = self.db["corpus"].find(stmt,{"value": 1, 'num_of_records': 1, "_id": 0})
    if freq:
      result = [ (doc["value"],doc['num_of_records']) for doc in docs ]
    else:
      result = [ doc["value"] for doc in docs ]
    return result

  def basic_query(self,query,query_ptid_list=None,result_type="list"):
    """
    query = {col_name1:{match stmt}, col_name2:{match stmt}, ...}
    """
    result = []
    for col_name,stmt in query.items():
      # mongo aggregation pipeline
      ap_stmt = [
        { "$match" : stmt},
        { "$project": { "ptid_list":1,"pt_group":1, "_id":0 }},
        { "$unwind" : "$ptid_list" },
        { "$unwind" : "$ptid_list" },
        { "$group": {
          "_id":"$pt_group",
          "ptid_list":{"$addToSet":"$ptid_list"}
          }
        }
      ]
      docs = self.db[col_name+"_tii"].aggregate(ap_stmt,allowDiskUse=True)
      for doc in docs:
        result+=doc["ptid_list"]
      
      docs.close()
    
    if query_ptid_list:
      result = list(set(result) & set(query_ptid_list))
    elif len(list(query.keys()))>1:
      result = list(set(result))

    return result

  @staticmethod
  def generate_absolute_temporal_constaints(after_time,before_time):
    temporal_constraints = {}
    if not after_time:
      after_time = datetime(2000,1,1)
    if not before_time:
      before_time = datetime(2030,1,1)
    temporal_constraints["date_list"] = {"$elemMatch": {"$gte":after_time,"$lte":before_time}}

    return temporal_constraints

  def absolute_temporal_query(self,query,query_period,query_ptid_list=None):
    query_period = [None,None] if not query_period else query_period
    after_time = query_period[0]
    before_time = query_period[1]
    result = []
    for col_name,match_stmt in query.items():
      if after_time or before_time:
        temporal_constraints = self.generate_absolute_temporal_constaints(after_time,before_time)
        _match_stmt = match_stmt.copy()
        _match_stmt.update(temporal_constraints)

      ap_stmt = [ 
        { "$match":_match_stmt },
        { "$project": { 
          "_id":0,
          "pt_group":1,
          "ptid_list":{
            "$slice": ["$ptid_list",
              {"$indexOfArray":[
                "$date_list",
                {"$first": {
                  "$filter": {
                    "input": "$date_list",
                    "as": "item",
                    "cond": { "$gte": ["$$item",after_time] }
                  }
                }}
              ]},
              { "$size": {
                "$filter": {
                  "input": "$date_list",
                  "as": "item",
                  "cond": {"$and": [ {"$gte": ["$$item",after_time]},{"$lte": ["$$item",before_time]}]}
                }
              }}
            ]
          }
        }},
        { "$unwind" : "$ptid_list" },
        { "$unwind" : "$ptid_list" },
        { "$group": {
          "_id":"$pt_group",
          "ptid_list":{"$addToSet":"$ptid_list"}
          }
        }
      ]
      docs = self.db[col_name+"_tii"].aggregate(ap_stmt,allowDiskUse=True)
      for doc in docs:
        result += doc["ptid_list"]
      docs.close()

    if query_ptid_list:
      result = list(set(result) & set(query_ptid_list))
    elif len(list(query.keys()))>1:
      result = list(set(result))
    return result

  def relative_temporal_query(self,query_a,query_b,query_period=None,query_ptid_list=None,return_type_a="first",return_type_b="last"):
    if (return_type_a not in ["first","last"] or return_type_b not in ["first","last"] ):
      print("return_type should be 'first' or 'last'. ")
      return []

    query_period = [None,None] if not query_period else query_period
    # step1: get patients have both a and b, using tii
    if query_period == [None,None]:
      a_ptid_list = self.basic_query(query_a)
      b_ptid_list = self.basic_query(query_b)
    else:
      a_ptid_list = self.absolute_temporal_query(query_a,query_period)
      b_ptid_list = self.absolute_temporal_query(query_b,query_period)
    if query_ptid_list:
      ab_ptid_list = list(set(a_ptid_list) & set(b_ptid_list) & set(query_ptid_list))
    else:
      ab_ptid_list = list(set(a_ptid_list) & set(b_ptid_list))
    if not ab_ptid_list:
      return []

    # step2: for each patient, get first or last event (depends on temporal logic) a and event b
    a_record_id_dict = self.record_id_query(ab_ptid_list,query_a,query_period,return_type=return_type_a)
    b_record_id_dict = self.record_id_query(ab_ptid_list,query_b,query_period,return_type=return_type_b)

    # step3: temporal comparison between event a and event b
    result_list = []
    for ptid,a_rid in a_record_id_dict.items():
      b_rid = b_record_id_dict[ptid]
      a_date = int(a_rid)
      b_date = int(b_rid)
      if b_date-a_date>=0:
        result_list.append(ptid)
      
    return result_list

  def record_id_query(self,ptid_list,query_dict,query_period,return_type="first"):
    temporal_constaints = []
    if query_period[0]:
      temporal_constaints.append({ "$gte":["$$rids", int('%d%02d%02d' % (query_period[0].year, query_period[0].month, query_period[0].day) )] })
    if query_period[1]:
      temporal_constaints.append({ "$lt":["$$rids", int('%d%02d%02d' % (query_period[1].year, query_period[1].month, query_period[1].day) )+1] })
    filtered_record_id_list = {
      "$filter": {
        "input": "$record_id_list",
        "as": "rids",
        "cond": { "$and": temporal_constaints }
      }
    }
    if return_type == "first":
      array_index = 0
      group_operator = "$min"
    elif return_type == "last":
      array_index = -1
      group_operator = "$max"
    result = {}
    for col_name,query in query_dict.items():
      col_record_id_list = []
      _query = query.copy()
      if ptid_list:
        _query.update({"PTID":{"$in":ptid_list}})
      if not temporal_constaints:
        ap_stmt =[
          { "$match" : _query },
          { "$project" : { "PTID":1,"record_id_list":{ "$arrayElemAt": [ "$record_id_list", array_index ] }, "_id":0 }},
          { "$group":
            {"_id": "$PTID",
            "record_id_list": {group_operator:"$record_id_list" } }
          }
        ]
      else:
        ap_stmt =[
          { "$match" : _query },
          { "$project" : { "PTID":1,"record_id_list":{ "$arrayElemAt": [ filtered_record_id_list, array_index ] }, "_id":0 }},
          { "$group":
            {"_id": "$PTID",
            "record_id_list": {group_operator:"$record_id_list" } }
          }
        ]
      docs = self.db[col_name+"_pt_timeline"].aggregate(ap_stmt,allowDiskUse=True)
      if return_type == "first":
        for doc in docs:
          try:
            result[doc["_id"]] = min(result[doc["_id"]],doc["record_id_list"])
          except:
            result[doc["_id"]] = doc["record_id_list"]
      elif return_type == "last":
        for doc in docs:
          try:
            result[doc["_id"]] = max(result[doc["_id"]],doc["record_id_list"])
          except:
            result[doc["_id"]] = doc["record_id_list"]

    return result


