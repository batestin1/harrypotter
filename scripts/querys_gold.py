#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: HarryPotter
#     Repositorio: stagin gold
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

print("Starting processing for gold file...")

###################extrac########################################

people = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/silver/people/").createOrReplaceTempView("people")
school = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/silver/school/").createOrReplaceTempView("school")
exam = spark.read.parquet("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/silver/exam/").createOrReplaceTempView("exam")

###################transform########################################



exam_final = spark.sql(""" SELECT
PE.id, PE.notebook, PE.first_name, PE.last_name, PE.race,
PE.birthday, PE.age, PE.civil_status,PE.phone, PE.occupation, 
SC.school, SC.country, SC.houses,
PE.patronus, PE.wand,
PE.identification_number, Pe.mailer,
EX.divination, EX.alchemy, EX.apparition, EX.arithmancy, EX.muggle_art, EX.dark_arts, EX.astronomy, EX.defense_against_the_dark_arts,
EX.study_of_ancient_runes, EX.muggle_study, EX.ancient_studies, EX.advanced_arithmancy_studies, 
EX.spells, EX.herbology, EX.history_of_magic, EX.general_knowledge_of_magic, EX.potions,
EX.theory_of_magic, EX.transfiguration, EX.care_of_magical_creatures, EX.media, Pe.yearmonthday
FROM people PE INNER JOIN school SC ON PE.id = SC.id INNER JOIN exam EX ON PE.id = EX.id
""")

###################load########################################
exam_final.write.mode("overwrite").format("parquet").partitionBy("yearmonthday").save("C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/stagin/gold/harrypotter")

