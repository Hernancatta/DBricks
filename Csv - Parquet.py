# Databricks notebook source
# MAGIC %md # Convertento arquivo csv em parquet

# COMMAND ----------

# MAGIC %md
# MAGIC -  **spark.read.format("csv")  Definindo o formato para leitura**
# MAGIC -  **.option("header", "True") Defindo cabeçalho** 
# MAGIC -    **.option("delimiter", ";")  Definir separador de colunas no arquivo** 
# MAGIC -    **.option("inferSchema", "True")  Inferir de forma automatica o tipo de dado de cada coluna**
# MAGIC -    **.load("/FileStore/tables/siconv_etapa_crono_fisico.csv")  Carregar arquivo e especificar o caminho** 

# COMMAND ----------

# Lendo arquivos csv do dbfs com Spark 
df = (
    spark.read.format("csv")\
    .option("header", "True")\
    .option("delimiter", ";")\
    .option("inferSchema", "True")\
    .load("/FileStore/tables/siconv_etapa_crono_fisico.csv") 
)

# COMMAND ----------

# Verificando o Schema do dataframe
df.printSchema()

# COMMAND ----------

# Verificar as 10 primeiras linhas
display(df.show(10))

# COMMAND ----------

# Contando a quantidade de linhas 
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leva os dados convertidos para Processing Zone

# COMMAND ----------

# MAGIC %md
# MAGIC - **df.write.format("parquet") Define o formato de saída do arquivo**
# MAGIC - **.mode("overwrite") Especifica o modo de gravação e sobrescrever os arquivo no diretório**
# MAGIC - **.save("/FileStore/tables/csv to parquet") Define o caminho onde os arquivos serão salvos**

# COMMAND ----------

# Converte para o formato parquet
df.write.format("parquet")\
.mode("overwrite")\
.save("/FileStore/tables/csv to parquet") 

# COMMAND ----------

# MAGIC %md
# MAGIC - **df_parquet = spark.read.format("parquet")\ Definindo um novo DataFrame** 
# MAGIC - **.load("/FileStore/tables/csv to parquet"  Define o caminho onde sera carregado** 

# COMMAND ----------

# Lendo arquivos parquet
df_parquet = spark.read.format("parquet")\
.load("/FileStore/tables/csv to parquet")

# COMMAND ----------

# conta a quantidade de linhas 
df_parquet.count()

# COMMAND ----------

display(df_parquet.head(10))
