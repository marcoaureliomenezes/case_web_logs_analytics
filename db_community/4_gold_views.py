# Databricks notebook source
# MAGIC %sql
# MAGIC -- Identifique as 10 maiores origens de acesso (Client IP) por quantidade de acessos.
# MAGIC SELECT ip_address, COUNT(*) AS occurences FROM silver.logs GROUP BY ip_address ORDER BY occurences DESC LIMIT 10;
# MAGIC
# MAGIC -- Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.
# MAGIC SELECT http_route, http_method, COUNT(*) AS occurences FROM silver.logs GROUP BY http_method, http_route ORDER BY occurences DESC LIMIT 6;
# MAGIC
# MAGIC -- Qual a quantidade de Client IPs distintos?
# MAGIC SELECT approx_count_distinct(ip_address) FROM silver.logs;
# MAGIC
# MAGIC -- Quantos dias de dados estão representados no arquivo?
# MAGIC SELECT DATEDIFF(MIN(response_utc_timestamp), MAX(response_utc_timestamp))  FROM silver.logs;
# MAGIC
# MAGIC -- Com base no tamanho (em bytes) do conteúdo das respostas, mostre "O volume total de dados retornado"
# MAGIC SELECT SUM(payload_size) FROM silver.logs;
# MAGIC
# MAGIC -- Com base no tamanho (em bytes) do conteúdo das respostas, mostre "O maior volume de dados em uma única resposta."
# MAGIC SELECT * FROM silver.logs WHERE payload_size = (SELECT MAX(payload_size) FROM silver.logs);
# MAGIC
# MAGIC -- Com base no tamanho (em bytes) do conteúdo das respostas, mostre "O menor volume de dados em uma única resposta."
# MAGIC SELECT * FROM silver.logs WHERE payload_size = (SELECT MAX(payload_size) FROM silver.logs);
# MAGIC
# MAGIC -- Com base no tamanho (em bytes) do conteúdo das respostas, mostre "O menor volume de dados em uma única resposta."
# MAGIC SELECT http_method, http_route, MEAN(payload_size) AS mean_payload_size FROM silver.logs GROUP BY http_method, http_route;
# MAGIC
