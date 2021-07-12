import pandas as pd
from fastapi import FastAPI
from uvicorn import run
import json
from APIRequest.Requester import Requester

app = FastAPI(title="Samantha")
requester = Requester()


@app.get('/')
async def home():
    return "hele"

# DONE
@app.get('/sectores_time_series')
async def sectores_time_series():
    """
    Return the count of crimes that occurred in a sector, by date.
    :return:
    """
    global requester
    response = await requester.sectores_time_series()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('sectores_time_series.csv')


#DONE FIX-ME
@app.get('/cuadrante_time_series')
async def cuadrantetime_series():
    global requester
    response = await requester.cuadrante_time_series()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('cuadrante_time_series.csv')


# DONE
@app.get('/count_of_crimes_ocurred_in_cuadrante')
async def count_of_crimes_ocurred_in_cuadrante():
    """
    Return the count of crimes that occurred in a cuadrante, by date.
    :return:
    """
    global requester
    response = await requester.count_of_crimes_ocurred_in_cuadrante()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('count_of_crimes_ocurred_in_cuadrante.csv')


# DONE
@app.get('/sum_of_crimes_occurred_in_sector')
async def sum_of_crimes_occurred_in_sector():
    """
    Return the sum of crimes that occurred in a particular or in all sectores for a specified period of time.
    By default it returns the sum of crimes during the last 12 months for all the sectores in the database.    :return:
    """
    global requester
    response = await requester.sum_of_crimes_occurred_in_sector()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('sum_of_crimes_occurred_in_sector.csv')


# DONE
@app.get('/change_in_crime_counts_for_period_at_cuadrante_level')
async def change_in_crime_counts_for_period_at_cuadrante_level():
    """
    Return the change in crime counts for a specified period of time at the cuadrante level
    By default it returns the change during the last 12 months
    """
    global requester
    response = await requester.change_in_crime_counts_for_period_at_cuadrante_level()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('change_in_crime_counts_for_period_at_cuadrante_level.csv')


# DONE
@app.get('/top_ranked_sectors_with_highest_crime_rates_for_period_of_time')
async def top_ranked_sectors_with_highest_crime_rates_for_period_of_time():
    """
    Return the top ranked sectors with the highest crime rates for a given period of time.
    When no date parameters are specified the top 5 cuadrantes are returned for the last 12 months
    (e.g. If July is the last date in the database then the period July 2018 to Aug 2017 will be analyzed).
    Crimes where no sector was specified (NO ESPECIFICADO) are ignored. All population data returned by this
    call is in persons/year and comes from the 2010 census
    """
    global requester
    response = await requester.top_ranked_sectors_with_highest_crime_rates_for_period_of_time()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('top_ranked_sectors_with_highest_crime_rates_for_period_of_time.csv')


# DONE
@app.get('/top_ranked_cuadrantes_with_highest_crime_counts_for_period_of_time')
async def top_ranked_cuadrantes_with_highest_crime_counts_for_period_of_time():
    """
    Return the top ranked cuadrantes with the highest crime counts for a given period of time.
    When no dates parameters are specified the top 5 cuadrantes for the last 12 months are returned
    (e.g. If July is the last date in the database, then the period July 2018 to Aug 2017 will be analyzed).
    All population data returned by this call is in persons/year and comes from the 2010 census
    """
    global requester
    response = await requester.top_ranked_cuadrantes_with_highest_crime_counts_for_period_of_time()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('top_ranked_cuadrantes_with_highest_crime_counts_for_period_of_time.csv')


# DONE
@app.get('/top_ranked_cuadrantes_where_crime_increased_most')
async def top_ranked_cuadrantes_where_crime_increased_most():
    """
    Return the top ranked cuadrantes where crime counts increased the most.
    When no date parameters are specified the top 5 cuadrantes are returned for the last 3 months compared with
    the same period during the previous year (e.g. July-May 2018 compared with July-May 2017).
     All population data returned by this call is in persons/year and comes from the 2010 census
    """
    global requester
    response = await requester.top_ranked_cuadrantes_where_crime_increased_most()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('top_ranked_cuadrantes_where_crime_increased_most.csv')


# DONE
@app.get('/sum_crimes_in_df')
async def sum_crimes_in_df():
    global requester
    response = await requester.sum_crimes_in_df()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('sum_crimes_in_df.csv')


@app.get('/sectores')
async def lista_sectores():
    global requester
    return await requester.lista_sectores()


@app.get('/cuadrantes')
async def lista_cuadrantes():
    global requester
    return await requester.lista_cuadrantes()


@app.get('/crimenes')
async def lista_crimenes():
    global requester
    return await requester.lista_crimes()


@app.get('/municipios')
async def lista_municipios():
    global requester
    return await requester.lista_municipios()


@app.get('/geojson_cuadrantes_delictivos')
async def geojson_cuadrantes_delictivos():
    global requester
    response = await requester.geojson_cuadrantes_delictivos()
    with open("cuadrantes_delictivos.json", "w") as outfile:
        json.dump(response, outfile)


@app.get('/geojson_sectores_delictivos')
async def geojson_sectores_delictivos():
    global requester
    response = await requester.geojson_sectores_delictivos()
    print(response)


@app.get('/geojson_municipios_delictivos')
async def geojson_municipios_delictivos():
    global requester
    response = await requester.geojson_municipios_delictivos()
    print(response)


# DONE
@app.get('/crimes_by_hour')
async def crimes_by_hour():
    global requester
    response = await requester.crimes_by_hour()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('crimes_by_hour.csv')


@app.get('/crimes_by_days')
async def crimes_by_days():
    global requester
    response = await requester.crimes_by_days()
    rows = response['rows']
    df = pd.DataFrame(rows)
    df.to_csv('crimes_by_days.csv')

# def config_routes():
# app.include_router()


def config():
    # config_routes()
    pass


def run_server():
    run(app=app)


def main():
    config()
    run_server()


if __name__ == '__main__':
    main()

else:
    config()
