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

  col_name, concept, value = "cov_diag", "DIAGNOSIS_CD", "U071"
  query_a = { col_name : { concept : value} }
  result1 = query_client.basic_query(query_a)
  num_of_patients = len(result1)
  p_timer.click()
  print("matched patients = ", num_of_patients)
  print("The query takes", p_timer.prev_time)

  query_period = [datetime(2020,7,1),datetime(2020,7,31)]
  result1 = query_client.absolute_temporal_query(query_a,query_period)
  num_of_patients = len(result1)
  p_timer.click()
  print("matched patients = ", num_of_patients)
  print("The query takes", p_timer.prev_time)


if __name__ == '__main__':
  mytest()