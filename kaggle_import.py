import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


username = 'iryna'
password = 'leaf'
database = 'lab2'
host = 'localhost'
port = '5432'
csv_name = "E:\KPI\Databases\Lab2\source csv\penguins_lter.csv"


def to_snake_case(line):
    line = line.lower()
    line = line.replace(" ", '_')
    return line


def to_bool(line):
    if line == 'Yes':
        return "TRUE"
    elif line == 'No':
        return "FALSE"
    else:
        return 'NULL'


def wrap_string(line):
    return "'" ++ line.to_string() ++ "'"


def to_string(line, skip_wrapping):
    string = []
    for index, value in line.iteritems():
        if (index in skip_wrapping) or (value == "NULL"):
            string.append(str(value))
        else:
            # print(value)
            string.append("'" + str(value) + "'")
    return ", ".join(string)


def to_datetime(line):
    string = line.split('/')
    return '20'+string[2]+'-'+string[0]+"-"+string[1]


data = pd.read_csv(csv_name)

data_islands = pd.DataFrame(data['Island'])
data_islands.set_index('Island', inplace=True)
data_islands.loc['Torgersen', 'Longtitude'] = -64.083333
data_islands.loc['Torgersen', 'Latitude'] = -64.766667
data_islands.loc['Dream', 'Longtitude'] = -64.233333
data_islands.loc['Dream', 'Latitude'] = -64.733333
data_islands.loc['Biscoe', 'Longtitude'] = -65.5
data_islands.loc['Biscoe', 'Latitude'] = -65.433333
data_islands = data_islands.drop_duplicates()

data = pd.merge(data, data_islands, left_on='Island', right_on='Island')


data = data.drop(columns=['Delta 15 N (o/oo)', 'Delta 13 C (o/oo)', 'Comments', 'Individual ID'])
data['Study Author'] = 'Dr. Kristen Gorman'
data['Study Program'] = 'Palmer Station Long Term Ecological Research Program'

data.rename(columns={'studyName':'Study Name', "Culmen Length (mm)":'Culmen Length', 'Culmen Depth (mm)':'Culmen Depth', 'Flipper Length (mm)':'Flipper Length', 'Body Mass (g)':'Body Mass' }, inplace=True)
data.columns = pd.Series(data.columns).apply(to_snake_case)

data['clutch_completion'] = data['clutch_completion'].apply(to_bool)
data.rename(columns={'study_name':'study_number', 'island':'island_name'}, inplace=True)
data['date_egg'] = data['date_egg'].map(to_datetime)



penguin_record = data.drop(columns=['region', 'longtitude', 'latitude', 'study_author', 'study_program'])

islands = data[['island_name', 'longtitude', 'latitude', 'region']].drop_duplicates()
islands.reset_index(inplace=True)
islands.drop(columns='index', inplace=True)

studies = data[['study_number', 'study_author', 'study_program']].drop_duplicates()
studies.reset_index(inplace=True)
studies.drop(columns='index', inplace=True)


delete_quary = '''DROP TABLE PenguinRecords CASCADE;
                  DROP TABLE Islands CASCADE;
                  DROP TABLE Studies CASCADE'''

create_quary = '''
CREATE TABLE PenguinRecords
(
  sample_number     int       NOT NULL ,
  species           varchar(255) NOT NULL ,
  island_name       varchar(50)  NOT NULL ,
  study_number      varchar(50)  NOT NULL ,
  stage             varchar(255) NULL ,
  clutch_completion bool      NULL ,
  date_egg          date      NULL ,
  culmen_length     float     NULL ,
  culmen_depth      float     NULL ,
  flipper_length    float     NULL ,
  body_mass         float     NULL ,
  sex               varchar(7)   NULL 
);

CREATE TABLE Islands
(
  island_name  varchar(50)   NOT NULL ,
  longtitude   float      NOT NULL ,
  latitude     float      NOT NULL ,
  region       varchar(50)   NOT NULL 
);


CREATE TABLE Studies
(
  study_number   varchar(50)   NOT NULL ,
  study_author   varchar(255)  NOT NULL ,
  study_program  varchar(225)  NULL 
);


ALTER TABLE PenguinRecords ADD PRIMARY KEY (species, sample_number);
ALTER TABLE Islands ADD PRIMARY KEY (island_name);
ALTER TABLE Studies ADD PRIMARY KEY (study_number);

ALTER TABLE PenguinRecords ADD CONSTRAINT FK_PenguinRecords_Islands FOREIGN KEY (island_name) REFERENCES Islands (island_name);
ALTER TABLE PenguinRecords ADD CONSTRAINT FK_PenguinRecords_Studies FOREIGN KEY (study_number) REFERENCES Studies (study_number);
'''

insert_quary_records = '''
INSERT INTO PenguinRecords(study_number, sample_number, species, island_name, stage, clutch_completion, date_egg, culmen_length, culmen_depth, flipper_length, body_mass, sex)
Values(%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s);
'''

insert_quary_studies = '''
INSERT INTO Studies(study_number, study_author, study_program)
Values(%s, %s, %s)
'''
insert_quary_islands = '''
INSERT INTO Islands(island_name, longtitude, latitude, region)
Values(%s, %s, %s, %s)
'''
penguin_record = penguin_record.astype({'sample_number': 'object'})

connection = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with connection:
    pointer = connection.cursor()
    pointer.execute(delete_quary)
    pointer.execute(create_quary)
    for i in range(len(islands)):
        pointer.execute(insert_quary_islands, islands.values.tolist()[i])

    for i in range(len(studies)):
        pointer.execute(insert_quary_studies, studies.values.tolist()[i])

    for i in range(len(penguin_record)):
        pointer.execute(insert_quary_records, penguin_record.values.tolist()[i])

    pointer.execute("SELECT * FROM penguinrecords")

    for row in pointer:
        print(row)

    connection.commit()

# print(penguin_record.dtypes)
