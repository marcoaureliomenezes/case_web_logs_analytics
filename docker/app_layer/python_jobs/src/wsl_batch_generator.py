from rand_engine.core.distinct_core import DistinctCore
from rand_engine.core.numeric_core import NumericCore
from rand_engine.core.datetime_core import DatetimeCore

from rand_engine.core.distinct_utils import DistinctUtils
from rand_engine.main.dataframe_builder import BulkRandEngine

from datetime import datetime as dt, timedelta

import faker
import csv

class WSLBatchGenerator:

  @staticmethod
  def metadata_case_web_log_server(format, dt_start, dt_end):
    fake = faker.Faker(locale='pt_BR')
    metadata = {
      "ip_address":dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=[fake.ipv4_public() for i in range(1000)])),
      "identificador": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=["-"])),
      "user": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=["-"])),
      "user_named": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=[fake.name() for i in range(1000)])),
      "datetime": dict(
        method=DatetimeCore.gen_datetimes, 
        parms=dict(start=dt_start, end=dt_end, format_in=format, format_out="%d/%b/%Y:%H:%M:%S")
      ),
      "http_version": dict(
        method=DistinctCore.gen_distincts_typed,
        parms=dict(distinct=DistinctUtils.handle_distincts_lvl_1({"HTTP/1.1": 7, "HTTP/1.0": 3}, 1))
      ),
      "campos_correlacionados_proporcionais": dict(
        method=       DistinctCore.gen_distincts_typed,
        splitable=    True,
        cols=         ["http_request", "http_status"],
        sep=          ";",
        parms=        dict(distinct=DistinctUtils.handle_distincts_lvl_3({
                          "GET /home": [("200", 7),("400", 2), ("500", 1)],
                          "GET /login": [("200", 5),("400", 3), ("500", 1)],
                          "POST /login": [("201", 4),("404", 2), ("500", 1)],
                          "GET /logout": [("200", 3),("400", 1), ("400", 1)],
                          "POST /signin": [("201", 4),("404", 2), ("500", 1)],
                          "GET /balance": [("200", 8),("404", 1), ("500", 1)],
                          "GET /statement": [("200", 8),("404", 1), ("500", 1)],
          }))
      ),
      "object_size": dict(method=NumericCore.gen_ints, parms=dict(min=0, max=10000)),
    }
    return metadata


  @staticmethod
  def web_server_log_transformer(df):
    # Regra para campo user: Quando campo http request contiver /home, /login, /logout então campo user = campo user_named. Caso contrário, campo user = '-'
    df['user'] = df.apply(lambda x: x['user_named'] if x['http_request'] in ['/home', '/login', '/logout'] else '-', axis=1)
    df = df['ip_address'] + ' ' + df['identificador'] + ' ' + df['user'] + ' [' + df['datetime'] + ' -0700] "' + \
                        df['http_request'] + ' ' + df['http_version'] + '" ' + df['http_status'] + ' ' + df['object_size'].astype(str)
    return df

  @staticmethod
  def handle_timestamp(dt_end, hours_back):
    dt_start = dt_end - timedelta(hours=hours_back)
    input_format = "%Y-%m-%d %H:%M:%S"
    return input_format, dt_start.strftime(input_format), dt_end.strftime(input_format)


  @staticmethod
  def generate_file_name(dt_start, dt_end):
    dt_start, dt_end = [dt.strptime(i, "%Y-%m-%d %H:%M:%S") for i in [dt_start, dt_end]]
    timestamp_start, timestamp_end = [int(dt.timestamp(i)) for i in [dt_start, dt_end]]
    return f"web_server_log_{timestamp_start}_{timestamp_end}.log"


  @staticmethod
  def generate_web_server_log_file(path, metadata, transformer, size=1000):
    bulk_engine = BulkRandEngine()
    df = bulk_engine.create_pandas_df(metadata=metadata, size=size)
    df = df.sort_values(by='datetime')
    df = transformer(df)
    df.to_csv(path, sep=' ', index=False, header=False, quoting=csv.QUOTE_NONE, escapechar=' ')
  
  
if __name__ == '__main__':
  pass
  
