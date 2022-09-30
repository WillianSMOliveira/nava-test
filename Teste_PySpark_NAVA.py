# Importacao das libraries
from datetime import datetime
from datetime import timedelta
import pyspark
import requests 
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
from pyspark.sql.functions import *
import sys
import os.path
from os import path
from io import StringIO

# Criando funcoes antes do bloco principal para poder ser reconhecida pelo Colab posteriormente
def getRddFromFile(spark, url, filename):
  # Comentado o bloco abaixo devido ao arquivo local nao ser acessivel por outras pessoas que acessarem este link aqui do Colab
  # rdd = spark.sparkContext.textFile(path) # ex: "/content/NAVA/tabela_saldo_inicial.txt"
  # return rdd

  # Utilizando um link para acessar o conteudo dos arquivos (criando um arquivo virtual utilizando SparkFiles)
  from pyspark import SparkFiles
  spark.sparkContext.addFile(url)
  rdd = spark.sparkContext.textFile("file://" + SparkFiles.get(filename))
  return rdd

def getDfMovimentacao(spark, url, fileName):
  # Obtém o rdd e popula os objetos com dados e cabecalho
  movimentacao_txt = getRddFromFile(spark, url, fileName)

  header_movimentacao_txt = movimentacao_txt.first()
  movimentacao_txt = movimentacao_txt.filter(lambda line: line != header_movimentacao_txt)
  temp_var = movimentacao_txt.map(lambda k: k.split(";"))
  movimentacao = temp_var.toDF(header_movimentacao_txt.split(";"))  

  # Converte para dataframe com os devidos tipos, altera nome das colunas
  df_movimentacao = movimentacao.withColumn("Movimentacao_dia",movimentacao["Movimentacao_dia"].cast(DoubleType())).\
  withColumn("data", func(col("data"))).\
  withColumnRenamed("Nome","nome").\
  withColumnRenamed("CPF","cpf").\
  withColumnRenamed("Movimentacao_dia","saldo")

  return df_movimentacao

def processaDados(df_saldo, df_movimentacao, process_date, process_date_menos_um, processou_dia_anterior = False):
  
  if(not processou_dia_anterior):
    # Se a data de processmento -1 estiver no arquivo de processamento, processa -1 antes
    filter_date = [process_date_menos_um]
    # Filtra o saldo do dia anterior para somar com os estornos que vieram no arquivo do dia atual
    df_saldo_menos_um = df_saldo.\
    filter(df_saldo.data.isin(filter_date))
    
    # Filtra os dados do dia anterior que vieram no arquivo do dia atual
    df_movimentacao_dia_menos_um = df_movimentacao.\
    filter(df_movimentacao.data.isin(filter_date))

    df_saldos = df_saldo_menos_um.union(df_movimentacao_dia_menos_um)
    
    # Remove saldo do dia anterior para que seja inserido o novo saldo reprocessado deste dia
    df_saldo = df_saldo.filter(f"data != '{process_date_menos_um}'")

    df_saldo = df_saldo.union(processaSaldo(df_saldos, process_date_menos_um))

    return df_saldo

  else:
    # Filtra o saldo do dia anterior
    df_saldo_anterior = df_saldo.filter(f"data == '{process_date_menos_um}'")
    # Filtra no df de movimentacao apenas os dados da data atual, pois do dia anterior ja foi processado e Unifica os dados do dia anterior com o do dia atual
    df_movimentacao = df_saldo_anterior.union(df_movimentacao.filter(f"data == '{process_date}'"))
    # Aciona a funcao que calcula a soma do dia anterior com a do dia de processamento e unifica o df de historico contendo o saldo do dia anterior e do dia processado
    df_saldo = df_saldo.union(processaSaldo(df_movimentacao, process_date))

    return df_saldo

def processaSaldo(df, process_date):
  df_processado = df.\
  groupBy("nome","cpf").\
  agg(sum("saldo").alias("saldo")).\
  withColumn("data", lit(process_date).cast(DateType()))
  return df_processado

# Inicializacao do pyspark e criacao dos objetos referente a carga inicial
spark = SparkSession.builder.appName("NAVA").getOrCreate()

saldo_inicial_txt = getRddFromFile(spark, "https://raw.githubusercontent.com/WillianSMOliveira/nava-test/main/tabela_saldo_inicial.txt", "tabela_saldo_inicial.txt")

header_saldo_inicial_txt = saldo_inicial_txt.first()
saldo_inicial_txt = saldo_inicial_txt.filter(lambda line: line != header_saldo_inicial_txt)
temp_var = saldo_inicial_txt.map(lambda k: k.split(";"))
saldo_inicial = temp_var.toDF(header_saldo_inicial_txt.split(";"))

# Funcao de conversao de string para data
func =  udf (lambda x: datetime.strptime(x, '%d/%m/%Y'), DateType())

# Converte para dataframe com os devidos tipos, altera nome das colunas e exibe informacoes
df_saldo = saldo_inicial.withColumn("Saldo_Inicial_CC",saldo_inicial["Saldo_Inicial_CC"].cast(DoubleType())).\
withColumn("data", func(col("data"))).\
withColumnRenamed("Nome","nome").\
withColumnRenamed("CPF","cpf").\
withColumnRenamed("Saldo_Inicial_CC","saldo")
df_saldo.printSchema()
df_saldo.sort(col("nome"),col("data")).\
select("nome", "saldo", date_format(col("data"), "dd/MM/yyyy").alias("data")).show()

# Obtem dados do arquivo do dia 02/04/2022

df_movimentacao = getDfMovimentacao(spark, "https://raw.githack.com/WillianSMOliveira/nava-test/main/movimentacao_dia_02_04_2022.txt", "movimentacao_dia_02_04_2022.txt")

# Processa arquivo do dia 02/04/2022

process_date = "2022-04-02"
date = datetime.strptime(process_date, '%Y-%m-%d')
process_date_menos_um = str((date - timedelta(days=1)).date())

# Verifica se e necessário processar o dia anterior, caso exista estorno para ser processado referente ao processamento atual
if(df_movimentacao.filter(df_movimentacao.data.contains(process_date_menos_um)).count() > 0):
  df_saldo = processaDados(df_saldo, df_movimentacao, process_date, process_date_menos_um, False)

df_saldo = processaDados(df_saldo, df_movimentacao, process_date, process_date_menos_um, True)

df_saldo.\
select("nome", "saldo", date_format(col("data"), "dd/MM/yyyy").alias("data")).\
sort("nome","data").show(100)

# Obtém dados do arquivo do dia 03/04/2022

df_movimentacao = getDfMovimentacao(spark, "https://raw.githack.com/WillianSMOliveira/nava-test/main/movimentacao_dia_03_04_2022.txt", "movimentacao_dia_03_04_2022.txt")

# Processa arquivo do dia 03/04/2022

process_date = "2022-04-03"
date = datetime.strptime(process_date, '%Y-%m-%d')
process_date_menos_um = str((date - timedelta(days=1)).date())

# Verifica se e necessário processar o dia anterior, caso exista estorno para ser processado referente ao processamento atual
if(df_movimentacao.filter(df_movimentacao.data.contains(process_date_menos_um)).count() > 0):
  df_saldo = processaDados(df_saldo, df_movimentacao, process_date, process_date_menos_um, False)

df_saldo = processaDados(df_saldo, df_movimentacao, process_date, process_date_menos_um, True)

df_saldo.\
select("nome", "saldo", date_format(col("data"), "dd/MM/yyyy").alias("data")).\
sort("nome","data").show(100)

