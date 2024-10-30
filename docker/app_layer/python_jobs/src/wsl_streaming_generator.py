from rand_engine.core.distinct_core import DistinctCore
from rand_engine.core.numeric_core import NumericCore
from rand_engine.core.datetime_core import DatetimeCore

from rand_engine.core.distinct_utils import DistinctUtils
from rand_engine.main.dataframe_builder import BulkRandEngine

import time



class WSLStreamingGenerator:

    
  def metadata_case_web_log_server(self):
    metadata = {
      "ip_address": dict(
        method=DistinctCore.gen_complex_distincts,
        parms=dict(
          pattern="x.x.x.x",  replacement="x", 
          templates=[
            {"method": DistinctCore.gen_distincts_typed, "parms": dict(distinct=["172", "192", "10"])},
            {"method": NumericCore.gen_ints, "parms": dict(min=0, max=255)},
            {"method": NumericCore.gen_ints, "parms": dict(min=0, max=255)},
            {"method": NumericCore.gen_ints, "parms": dict(min=0, max=128)}
          ]
        )),
      "identificador": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=["-"])),
      "user": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=["-"])),
      "datetime": dict(method=DistinctCore.gen_distincts_typed, parms=dict(distinct=["datetime"])),
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
                          "GET /logout": [("200", 3),("400", 1), ("400", 1)]
          }))
      ),
      "object_size": dict(method=NumericCore.gen_ints, parms=dict(min=0, max=10000)),
    }
    return metadata


  def web_server_log_transformer(self):
    return lambda df: df['ip_address'] + ' ' + df['identificador'] + ' ' + df['user'] + ' [' + df['datetime'] + ' -0700] "' + \
                        df['http_request'] + ' ' + df['http_version'] + '" ' + df['http_status'] + ' ' + df['object_size'].astype(str)



  def transform_in_flight(self, record, replacement):
    datetime_now = time.strftime("%d/%b/%Y:%H:%M:%S", time.localtime())
    return record.replace(replacement, datetime_now)


  def create_streaming_series(self, rand_engine, metadata, transformer, throughput=1000):
    while True:
      microbatch = rand_engine.create_pandas_df(metadata=metadata, size=throughput*10)
      microbatch = transformer(microbatch)
      for record in microbatch:
        datetime_now = time.strftime("%d/%b/%Y:%H:%M:%S", time.localtime())
        yield record.replace("datetime", datetime_now)
        time.sleep(1/throughput)


def generate_streaming_logs():
  web_server_log_generator = WebServerLogStreamingGenerator()
  bulk_rand_engine = BulkRandEngine()
  metadata = web_server_log_generator.metadata_case_web_log_server()
  transformer = web_server_log_generator.web_server_log_transformer()
  streaming_series = web_server_log_generator.create_streaming_series(bulk_rand_engine, metadata, transformer, throughput=1000)
  for record in streaming_series:
    print(record)

  

if __name__ == '__main__':

  generate_streaming_logs()