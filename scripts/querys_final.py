#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Harry Potter
#     Repositorio: output/SQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import mysql.connector
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')
#connection
bank = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = bank.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS harrypotter')
my_conn = create_engine('mysql+mysqldb://root:@localhost/harrypotter?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')


#################################CONFIGURE################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()



print("Starting processing for output file...")

###################extrac########################################

harrypotter = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/gold/harrypotter").createOrReplaceTempView("harrypotter")

###################transform########################################

harry = spark.sql("SELECT * FROM harrypotter")


resume_county = spark.sql("""SELECT metric, amount, score FROM(
SELECT 1 as index, 'Total of Participants' metric, COUNT(distinct id) as amount, 'null' as score FROM harrypotter UNION
SELECT 2 as index, 'Total of Brazil' metric, COUNT(country)  as amount, SUM(media) as score FROM harrypotter where country = 'BRAZIL' UNION
SELECT 3 as index, 'Total of Japan' metric, COUNT(country)  as amount, SUM(media) as score FROM harrypotter where country = 'JAPAN' UNION
SELECT 4 as index, 'Total of Africa' metric, COUNT(country)  as amount,SUM(media) as score  FROM harrypotter where country = 'AFRICA' UNION
SELECT 5 as index, 'Total of EUA' metric, COUNT(country)  as amount, SUM(media) as score  FROM harrypotter where country = 'EUA' UNION
SELECT 6 as index, 'Total of Norway' metric, COUNT(country)  as amount, SUM(media) as score  FROM harrypotter where country = 'NORWAY' UNION
SELECT 7 as index, 'Total of France' metric, COUNT(country)  as amount,SUM(media) as score FROM harrypotter where country = 'FRANCE' UNION
SELECT 8 as index, 'Total of Russian' metric, COUNT(country)  as amount, SUM(media) as score  FROM harrypotter where country = 'RUSSIAN' UNION
SELECT 9 as index, 'Total of England' metric, COUNT(country)  as amount, SUM(media) as score  FROM harrypotter where country = 'ENGLAND'
)

""")

resume_hogwarts = spark.sql("""SELECT metric, amount, score FROM(
SELECT 1 as index, 'Total of Participents of Hogwarth' metric, COUNT(country) as amount, 'null' as score FROM harrypotter WHERE country = "ENGLAND" UNION
SELECT 2 as index, 'Total of Gryffindor' metric, COUNT(houses)  as amount, SUM(media) as score  FROM harrypotter where houses = 'GRYFFINDOR' UNION
SELECT 3 as index, 'Total of Hufflepuff' metric, COUNT(houses)  as amount, SUM(media) as score  FROM harrypotter where houses = 'HUFFLEPUFF'  UNION
SELECT 4 as index, 'Total of Ravenclaw' metric, COUNT(houses)  as amount, SUM(media) as score   FROM harrypotter where houses = 'RAVENCLAW'  UNION
SELECT 5 as index, 'Total of Slytherin' metric, COUNT(houses)  as amount, SUM(media) as score   FROM harrypotter where houses = 'SLYTHERIN'
) 

""")


###################################Load########################

harry.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/harrypotter/output/raw/harrypotter")
resume_county.write.mode("overwrite").format("parquet").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/harrypotter/output/resume/country")
resume_hogwarts.write.mode("overwrite").format("parquet").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/harrypotter/output/resume/hogwwarth")
harry.write.format('jdbc').options(url='jdbc:mysql://localhost/harrypotter?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='exam',user='root',password='').mode('append').save()
resume_county.write.format('jdbc').options(url='jdbc:mysql://localhost/harrypotter?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='resume_country',user='root',password='').mode('append').save()
resume_hogwarts.write.format('jdbc').options(url='jdbc:mysql://localhost/harrypotter?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='resume_hogwarts',user='root',password='').mode('append').save()