class BreweriesDDL:

  def __init__(self, spark, logger):
    self.spark = spark
    self.logger = logger

  def create_namespace(self, namespace):
    self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    self.logger.info(f"Namespace {namespace} created")
    return True


  def create_bronze_table(self, table_name, table_path):
    self.spark.sql(f"""
      CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
      value STRING    NOT NULL COMMENT 'Raw log line',
      date_ref STRING NOT NULL COMMENT 'Date reference')
    USING iceberg
    LOCATION '{table_path}'
    PARTITIONED BY (date_ref)
    TBLPROPERTIES ('gc.enabled' = 'true')""")
    self.spark.table(table_name).printSchema()
    self.logger.info(f"Table {table_name} created")
    return True
  

  def create_silver_table(self, table_name, table_path):
    self.spark.sql(f"""
      CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
      ip_address STRING                 NOT NULL COMMENT 'IP Address',                                                                                                                                                                              ,
      user STRING                       NOT NULL COMMENT 'User authenticated',                                                                                                                                                                                      ,
      response_utc_timestamp TIMESTAMP  NOT NULL COMMENT 'Response timestamp in UTC',                                                                                                                                                                                      ,
      http_method STRING                NOT NULL COMMENT 'HTTP Method',                                                                                                                                                                                      ,
      http_route STRING                 NOT NULL COMMENT 'HTTP Route',                                                                                                                                                                                      ,
      http_protocol STRING              NOT NULL COMMENT 'HTTP Protocol',                                                                                                                                                                                      ,
      http_status INT                   NOT NULL COMMENT 'HTTP Status',                                                                                                                                                                                      ,
      payload_size INT                  NOT NULL COMMENT 'Payload size',                                                                                                                                                                                      ,
      date_ref STRING                   NOT NULL COMMENT 'Date reference')
    USING iceberg
    PARTITIONED BY (date_ref)
    LOCATION '{table_path}'
    TBLPROPERTIES ('gc.enabled' = 'true')""")
    self.spark.table(table_name).printSchema()
    self.logger.info(f"Table {table_name} created")
    return True

  
  def create_gold_view_1(self, spark):
    # Identifica as 10 maiores origens de acesso (Client IP) por quantidade de acessos.
    view_name = "nessie.gold.top_10_client_ips"
    spark.sql(f"""
      CREATE OR REPLACE VIEW {view_name} AS
      SELECT ip_address, COUNT(*) AS occurences FROM silver.logs GROUP BY ip_address ORDER BY occurences DESC LIMIT 10""")
    return self
  

  def create_gold_view_2(self, spark):
    # Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.
    view_name = "nessie.gold.top_6_endpoints"
    spark.sql(f"""
      CREATE OR REPLACE VIEW {view_name} AS
      SELECT http_route, http_method, COUNT(*) AS occurences FROM silver.logs GROUP BY http_method, http_route ORDER BY occurences DESC LIMIT 6""")
    

  def create_gold_view_3(self, spark):
    # Qual a quantidade de Client IPs distintos?
    view_name = "nessie.gold.distinct_client_ips"
    spark.sql(f"""
      CREATE OR REPLACE VIEW {view_name} AS
      SELECT approx_count_distinct(ip_address) FROM silver.logs""")


  def create_gold_view_4(self, spark):
    # Quantos dias de dados estão representados no arquivo?
    view_name = "nessie.gold.log_days_represented"
    spark.sql(f"""
      CREATE OR REPLACE VIEW {view_name} AS
      SELECT DATEDIFF(MIN(response_utc_timestamp), MAX(response_utc_timestamp))  FROM silver.logs""")
    

  def create_gold_view_5_1(self, spark):
    # Com base no tamanho (em bytes) do conteúdo das respostas, mostre "O volume total de dados retornado.".
    view_name = "nessie.gold.total_traffic"
    spark.sql(f"""
      CREATE OR REPLACE VIEW {view_name} AS
      SELECT SUM(payload_size) FROM silver.logs;""")
    
  def create_gold_view_5_2(self, spark):
    # Com base no tamanho (em bytes) do conteúdo das respostas, mostre "O maior volume de dados em uma única resposta".
    view_name = "nessie.gold.biggest_payload"
    spark.sql(f"""
      CREATE OR REPLACE VIEW {view_name} AS
      SELECT * FROM silver.logs WHERE payload_size = (SELECT MAX(payload_size) FROM silver.logs)""")
    
  def create_gold_view_5_3(self, spark):
    # Com base no tamanho (em bytes) do conteúdo das respostas, mostre "O menor volume de dados em uma única resposta".
    view_name = "nessie.gold.smallest_payload"
    spark.sql(f"""
      CREATE OR REPLACE VIEW {view_name} AS
      SELECT * FROM silver.logs WHERE payload_size = (SELECT MIN(payload_size) FROM silver.logs)""")
    
  def create_gold_view_5_4(self, spark):
  # O volume médio de dados retornado. Dica: Considere como os dados podem ser categorizados por tipo de resposta para realizar essas análises.
    spark.sql(f"""
      CREATE OR REPLACE VIEW gold_view_5_3 AS
      SELECT AVG(payload_size) FROM silver.logs""")
    
  def create_gold_view_6(self, spark):
    # 6. Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?
    spark.sql(f"""
      CREATE OR REPLACE VIEW gold_view_6 AS
      SELECT date_format(response_utc_timestamp, 'EEEE') AS day_of_week, COUNT(*) AS occurences
      FROM silver.logs
      WHERE http_status >= 400 AND http_status < 500
      GROUP BY day_of_week
      ORDER BY occurences DESC LIMIT 1""")
