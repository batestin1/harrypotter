#!/usr/local/bin/python3
#coding: utf-8
#PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Harry Potter
#     Repositorio: Json
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports

import json
import csv
from faker import Faker
import faker_commerce
import faker_microservice
from faker_vehicle import VehicleProvider
from faker_music import MusicProvider
import random
from datetime import date, datetime
from create_data import Variables
import pymongo
from pymongo import MongoClient
client = pymongo.MongoClient('localhost', 27017)
faker = Faker()
#variables

db = client['harrypotter']
Collection = db["exam"]
print("""
 
   ##       ######   ######   ######   ##  ##                     ######   ######   ######   ######   ######
 ##       ######   ##  ##   ##  ##   ##  ##            ######   ######   ######   ######   ######   ##  ##
 ######   ##  ##   ##  ##   ##  ##   ##  ##            ##  ##   ##  ##     ##       ##     ##       ##  ##
 ##  ##   ######   ######   ######   ######            ##  ##   ##  ##     ##       ##     ####     ######
 ##  ##   ##  ##   #####    #####      ##              ######   ##  ##     ##       ##     ##       #####
 ##  ##   ##  ##   ##  ##   ##  ##    ####             ##       ######     ##       ##     ######   ##  ##
                                                       ##

                                                        
                     Test your knowledge of magic!
        - Big Data Project that simulates the Harry Potter Exam -                                              

""")


val = int(input("INSERT THE AMOUNT OF DATA TO BE GENERATED: "))
Variables(val)
num = 0
for i in range(val):
    num = num + 1
    with open(f'C:/Users/Bates/Documents/Repositorios/BIGDATA/HARRYPOTTER/dataset/json_files/tables_{num}.json') as file:
         exam = json.load(file)
         Collection.insert_one(exam)
       

