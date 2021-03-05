import sys
sys.path.insert(0, '..')

from datetime import datetime

from Query.query import QueryClient
from Utils.timer import Timer
import config.cred as cred


def mytest():
  username = cred.USERNAME
  password = cred.PASSWORD
  host = cred.HOST
  port = cred.PORT
  db_name = cred.DB_NAME
  mongo_url = "mongodb://"+username+":"+password+"@"+host+":"+port+"/?authSource="+db_name+"&authMechanism=SCRAM-SHA-256"
  query_client = QueryClient(mongo_url,db_name)

  p_timer = Timer()

  query_a = { "cov_diag" : { "DIAGNOSIS_CD" : "U071"} }
  print("Patients who had U071 diagnosis")
  result1 = query_client.basic_query(query_a)
  num_of_patients = len(result1)
  p_timer.click()
  print("matched patients = ", num_of_patients)
  print("first 5 results:", result1[:5])
  print("The query takes", p_timer.prev_time)

  query_period = [datetime(2020,7,1),datetime(2020,7,31)]
  print("Patients who had U071 diagnosis in July 2020")
  result2 = query_client.absolute_temporal_query(query_a,query_period)
  num_of_patients = len(result2)
  p_timer.click()
  print("matched patients = ", num_of_patients)
  print("first 5 results:", result2[:5])
  print("The query takes", p_timer.prev_time)

  query_b = { "cov_carearea": {"CAREAREA":"CRITICAL CARE UNIT (CCU) / INTENSIVE CARE UNIT (ICU)" } }
  print("Patients who had any ICU visit after first U071 diagnosis")
  result3 = query_client.relative_temporal_query(query_a,query_b,query_period=None,query_ptid_list=None,return_type_a="first",return_type_b="last")
  num_of_patients = len(result3)
  p_timer.click()
  print("matched patients = ", num_of_patients)
  print("first 5 results:", result3[:5])
  print("The query takes", p_timer.prev_time)


if __name__ == '__main__':
  mytest()