#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Harry Potter
#     Repositorio: stagin bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting processing for Silver file...")

###################extrac########################################

people = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/bronze/people/").createOrReplaceTempView("people")
school = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/bronze/school/").createOrReplaceTempView("school")
exam = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/bronze/exam/").createOrReplaceTempView("exam")


######################deduplication##################################
people = spark.sql(""" SELECT l. 
* FROM (SELECT *, row_number() over 
(partition by id order by yearmonthday desc) as row_id FROM people WHERE TRIM(id) <> '') l WHERE row_id = 1""")

school = spark.sql(""" SELECT l. 
* FROM (SELECT *, row_number() over 
(partition by id order by yearmonthday desc) as row_id FROM school WHERE TRIM(id) <> '') l WHERE row_id = 1""")
exam = spark.sql(""" SELECT l. 
* FROM (SELECT *, row_number() over 
(partition by id order by yearmonthday desc) as row_id FROM exam WHERE TRIM(id) <> '') l WHERE row_id = 1""")


people.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/silver/people/")
school.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/silver/school/")
exam.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/silver/exam/")
