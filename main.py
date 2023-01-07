import psycopg2
import matplotlib.pyplot as plt
import numpy as np

username = 'iryna'
password = 'leaf'
database = 'lab2'
host = 'localhost'
port = '5432'

connection = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

print(connection)

query1 = '''CREATE VIEW IslandsPopulations AS
SELECT TRIM(island_name), COUNT(sample_number) as penguin_quantity FROM penguinrecords
GROUP BY island_name'''

query2 = '''CREATE VIEW CompletedClutchesPercentage AS
SELECT clutch_completion, COUNT(sample_number) AS penguin_quantity FROM penguinrecords
GROUP BY clutch_completion'''

query3 = '''CREATE VIEW CulmenLengthMassKorrelationAdelie AS
SELECT culmen_length, body_mass FROM penguinrecords
WHERE species = 'Adelie Penguin (Pygoscelis adeliae)' '''

data1 = []

with connection:

    pointer = connection.cursor()
    pointer.execute('DROP VIEW IF EXISTS IslandsPopulations')
    pointer.execute('DROP VIEW IF EXISTS CompletedClutchesPercentage')
    pointer.execute('DROP VIEW IF EXISTS CulmenLengthMassKorrelationAdelie')
    
    pointer.execute(query1)
    pointer.execute("SELECT * FROM IslandsPopulations")
    data1 = pointer.fetchall()
    island_names = [element[0] for element in data1]
    island_penguin_quantity = [element[1] for element in data1]

    print(island_names, island_penguin_quantity)
    # island_names = [data1[i][0] for i in range(len(data1))]
    # island_penguin_quantity = [data1[i][1] for i in range(len(data1))]
    # island_names = [data1[i][0] for i in range(len(data1))]
    # island_penguin_quantity = [data1[i][1] for i in range(len(data1))]


    pointer.execute(query2)
    pointer.execute("SELECT * FROM CompletedClutchesPercentage")
    data1 = pointer.fetchall()
    clutch_state = [element[0] for element in data1]
    clutch_penguin_quantity = [element[1] for element in data1]

    pointer.execute(query3)
    pointer.execute("SELECT * FROM CulmenLengthMassKorrelationAdelie")
    data1 = pointer.fetchall()
    culmen_lengthes = [element[0] for element in data1]
    body_masses = [element[1] for element in data1]
    

    
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3)
    fig.set_figwidth(16)
    fig.set_figheight(9)
    
    
    ax1.bar(island_names, island_penguin_quantity, zorder=4, color='#493657')
    ax1.set_title('Розподіл по островах')
    ax1.set_ylabel('Популяція')
    ax1.grid(color = '#DFF3E3', zorder = 0, axis='y')
    
    
    wedges, text= ax2.pie(clutch_penguin_quantity, colors=['#FFD1BA', '#BEE5BF'], explode=[0, 0.1])
    ax2.set_title('Розподіл кладок')
    ax2.legend(wedges, ['неповна кладка\n(1 яйце)', 'повна кладка\n(2 яйця)'], loc='lower center', bbox_to_anchor=(0.35, -0.2))

    
    ax3.scatter(culmen_lengthes, body_masses, c='#95556B', zorder=4)
    ax3.set_title('Зв\'язок довжин клювів та ваги')
    ax3.set_xlabel('Довжина клюва (мм)')
    ax3.set_ylabel('Вага пінгвіна (г)')
    ax3.set_yticks(np.linspace(2800, 4750, 14))
    ax3.grid(color = '#DFF3E3', zorder = 0)
    
    plt.show()