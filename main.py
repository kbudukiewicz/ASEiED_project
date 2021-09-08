import matplotlib.pyplot as plt
import math as m
import tabulate as tab
from datetime import datetime

plik = open('Dane/green_tripdata_2019-05.csv', "r", encoding="Windows-1250")
tekst = plik.read()
podzial = tekst.split(",")
pick_up = list()
drop_off = list()
trip_distance = list()
#dla green
for i in range(20,len(podzial),1):
    if (i%19 == 1):
        pick_up.append(podzial[i])
    elif (i%19 == 2):
        drop_off.append(podzial[i])
    elif (i%19 == 8):
        trip_distance.append(podzial[i])

tablica_danych = list()
for i in range(len(trip_distance)):
    tablica_danych.append([pick_up[i], drop_off[i], float(trip_distance[i])])

print("Dane wczytne z pliku:")
#print(len(trip_distance))
#for i in range(len(trip_distance)):
#    print(trip_distance[i])
#print(tab.tabulate(tablica_danych, tablefmt="github"))

def __datetime(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

czas = list()

for i in range(len(trip_distance)):
    start_date = tablica_danych[i][0]
    end_date = tablica_danych[i][1]

    start = __datetime(start_date)
    end = __datetime(end_date)

    delta = end - start
    czas.append(delta.total_seconds()/3600)

srednie_predkosci = list()

for i in range(len(trip_distance)):
    if (czas[i]>0 and tablica_danych[i][2]>0):
        srednia_predkosc = tablica_danych[i][2]/czas[i]
        if (srednia_predkosc < 122):
            srednie_predkosci.append(tablica_danych[i][2]/czas[i])

#for i in range(len(srednie_predkosci)):
#    print(srednie_predkosci[i])

suma_srednich_predkosci = sum(srednie_predkosci)

print("Średnia prędkość taksówek to: {k} mph".format(k = suma_srednich_predkosci/len(srednie_predkosci)))