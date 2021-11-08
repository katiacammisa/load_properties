import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extensions import register_adapter


def save_data(df, connection):
    cursor = connection.cursor()

    for index in df.index:
        if df.loc[index, 'operation'] == 'sell':
            df.loc[index, 'operation'] = 'SALE'
        if df.loc[index, 'operation'] == 'rent':
            df.loc[index, 'operation'] = 'RENT'

    id_string = 1

    for index in df.index:
        print(index)
        id_string = id_string + 1
        street = df.loc[index, 'street']
        number = df.loc[index, 'housenumber']
        state = df.loc[index, 'state']
        city = df.loc[index, 'city'].upper()
        lon = df.loc[index, 'lon']
        lat = df.loc[index, 'lat']
        cursor.execute("SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326)", (lon, lat))
        coordinatesRaw = cursor.fetchall()
        coordinates = list(coordinatesRaw)[0][0]
        cursor.execute("select city.id from city join state s on city.state_id = s.id where city.name=%s and s.name=%s",
                       (city, state))
        cityIdRaw = cursor.fetchall()
        cursor.execute("select id from user_data where user_data.user_name='admin'")
        adminIdRaw = cursor.fetchall()
        adminId = list(adminIdRaw)[0][0]
        cursor.execute("select id from style where style.label='Contemporaneo'")
        styleIdRaw = cursor.fetchall()
        styleId = list(styleIdRaw)[0][0]
        if len(cityIdRaw) == 1:
            cityId = list(cityIdRaw)[0][0]
            cursor.execute(
                "INSERT INTO address (id, coordinates, number , street, city_id) VALUES (%s, %s, %s , %s, %s)",
                (str(id_string), coordinates, number, street, cityId))
            connection.commit()
            values = (
                str(id_string), ' ', df.loc[index, 'operation'], int(2015), df.loc[index, 'surface_covered_in_m2'],
                '2021-09-12', df.loc[index, 'rooms'] + 1, 500, df.loc[index, 'bathrooms'], 1, ' ',
                df.loc[index, 'price_aprox_usd'], df.loc[index, 'rooms'], df.loc[index, 'surface_total_in_m2'], 7,
                df.loc[index, 'title'], 1, df.loc[index, 'property_type'], str(id_string), adminId, styleId,
                bool(False))
            cursor.execute(
                "INSERT INTO property (id, comments, condition, construction_date, covered_square_foot, creation_date, environments, expenses, full_baths, levels, park_description, price, rooms, square_foot, step, title, toilets, type, address_id, owner_id, style_id, is_opportunity) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                values)
            connection.commit()


if __name__ == '__main__':
    connection = psycopg2.connect(host="localhost", database="ubicar", user="root", password="password")
    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

    df = pd.read_csv("../load_properties/csv/readyToUpload0.csv")
    for i in range(1, 16):
        df_aux = pd.read_csv("csv/readyToUpload" + str(i) + ".csv")
        df = df.append(df_aux, ignore_index=True)

    save_data(df, connection)
