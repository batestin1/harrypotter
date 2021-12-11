#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: HarryPotter
#     Repositorio: stagin bronze
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
# imports
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as sfunc
import pyspark.sql.types as stypes
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)


spark = SparkSession.builder.appName("MyApp").config("spark.mongodb.input.uri", "mongodb://localhost:27017").config("spark.mongodb.output.uri", "mongodb://localhost:27017").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()

# extract
df = spark.read.format("mongo").option("database", "harrypotter").option("collection", "exam").load()

#transform
df.createOrReplaceTempView('df')


#people
people = spark.sql("""SELECT
TEST_REGISTRATION as id, NOTEBOOK as notebook,
initcap(lower(split(trim(STUDENT_DATA.NAME),' ')[0])) as first_name,
initcap(lower(split(trim(STUDENT_DATA.NAME),' ')[1])) as last_name,
STUDENT_DATA.GENDER as gender, STUDENT_DATA.RACE as race, 
DATE_FORMAT(STUDENT_DATA.BIRTHDAY,'yyyy/mm/dd') as birthday,
cast(STUDENT_DATA.AGE as integer) as age,
STUDENT_DATA.CIVIL_STATUS as civil_status,
STUDENT_DATA.PHONE as phone,
STUDENT_DATA.OCCUPATION as occupation,
STUDENT_DATA.NU_IDENTIFICATION as identification_number,
STUDENT_DATA.MAILER as mailer,
STUDENT_DATA.PATRONUS as patronus,
IFNULL(STUDENT_DATA.WAND, 'null') as wand,
DATE_FORMAT(CURRENT_DATE, 'yyyymmdd')  as yearmonthday


FROM df""")

school = spark.sql("""SELECT
TEST_REGISTRATION as id, NOTEBOOK as notebook,
SCHOOL_DATA.SCHOOL as school,
SCHOOL_DATA.COUNTRY as country,
IFNULL(SCHOOL_DATA.HOUSES, 'null' ) as houses,
DATE_FORMAT(CURRENT_DATE, 'yyyymmdd')  as yearmonthday
FROM df""")

exam = spark.sql("""SELECT
TEST_REGISTRATION as id, NOTEBOOK as notebook,
EXAM_DATA.SUBJECTS.DIVINATION as divination,
EXAM_DATA.SUBJECTS.ALCHEMY as alchemy,
EXAM_DATA.SUBJECTS.APPARITION as apparition,
EXAM_DATA.SUBJECTS.ARITHMANCY as arithmancy,
EXAM_DATA.SUBJECTS.MUGGLE_ART as muggle_art,
EXAM_DATA.SUBJECTS.DARK_ARTS as dark_arts,
EXAM_DATA.SUBJECTS.ASTRONOMY as astronomy,
EXAM_DATA.SUBJECTS.DEFENSE_AGAINST_THE_DARK_ARTS as defense_against_the_dark_arts,
EXAM_DATA.SUBJECTS.STUDY_OF_ANCIENT_RUNES as study_of_ancient_runes,
EXAM_DATA.SUBJECTS.MUGGLE_STUDY as muggle_study,
EXAM_DATA.SUBJECTS.ANCIENT_STUDIES as ancient_studies,
EXAM_DATA.SUBJECTS.ADVANCED_ARITHMANCY_STUDIES as advanced_arithmancy_studies,
EXAM_DATA.SUBJECTS.SPELLS as spells,
EXAM_DATA.SUBJECTS.HERBOLOGY as herbology,
EXAM_DATA.SUBJECTS.HISTORY_OF_MAGIC as history_of_magic,
EXAM_DATA.SUBJECTS.DEFAULT as general_knowledge_of_magic,
EXAM_DATA.SUBJECTS.POTIONS as potions,
EXAM_DATA.SUBJECTS.THEORY_OF_MAGIC as theory_of_magic,
EXAM_DATA.SUBJECTS.TRANSFIGURATION as transfiguration,
EXAM_DATA.SUBJECTS.CARE_OF_MAGICAL_CREATURES as care_of_magical_creatures,
EXAM_DATA.MEDIA as media, 
DATE_FORMAT(CURRENT_DATE, 'yyyymmdd')  as yearmonthday
FROM df""")

###########################LOAD###########################
people.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/bronze/people/")
school.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/bronze/school/")
exam.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/bronze/exam/")
