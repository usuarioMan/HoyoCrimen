from Client.client import get_client


class Requester:
    """
    Singleton Requester.
    """
    __requester = None

    def __new__(cls) -> "Requester":
        if cls.__requester is None:
            cls.__requester = object.__new__(cls)
            cls.__requester.client = get_client()

        return cls.__requester

    # ============ POINT IN POLYGON ===============
    # Given a longitude and latitude return the corresponding cuadrante and sector.
    # Im going to skip this one becouse we don't have lat or long values to make the query.

    # ============ LATITUDE AND LONGITUDE ===============
    # Return the latitude and longitude of crimes
    # Given a latitude and longitude return all crimes within a certain distance.
    # Im going to skip this one becouse we don't have lat or long values to make the query.

    # ============ TIME SERIES ===============
    # Crime counts ordered by month of occurrence for a cuadrante or sector.

    # -----------------------------------------------------------------
    # /api/v1/sectores/(string: sector)/crimes/(string: crime)/series
    # Return the count of crimes that occurred in a sector, by date
    # No retorna datos antes de 2019 por lo que pasar parámetros para determinar fechas no tiene sentido.
    async def sectores_time_series(self):
        """
        Time Series. Crime counts ordered by month of occurrence for sectors.
        Return the count of crimes that occurred in a sector, by date.

        De todas formas, no es capaz de rellenar los datos.
        :return:
        """
        # date_params = dict(
        #     start_date="2016-01",
        #     end_date="2021-01",
        # )

        response = await self.client.async_client.get("https://hoyodecrimen.com/api/v1/sectores/all/crimes/all/series",
                                                      timeout=3000000)
        return response.json()

    # -----------------------------------------------------------------
    # /api/v1/cuadrantes/(string: cuadrante)/crimes/(string: crime)/series
    # Return the count of crimes that occurred in a cuadrante, by date
    async def cuadrante_time_series(self):
        """
        Time Series. Return the count of crimes that occurred in a cuadrante, by date.
        :return:
        """
        try:
            # date_params = dict(
            #     start_date="2019-01",
            #     end_date="2020-12",
            # )
            response = await self.client.async_client.get('/api/v1/cuadrantes/all/crimes/all/series',
                                                          timeout=3000000)
            # TODO: Page not working.
            return response.json()

        except Exception as cuadrante_time_series_error:
            print(cuadrante_time_series_error)
            pass

    # ============ LIST CUADRANTES OR SECTORES ===============

    # -----------------------------------------------------------------
    # Sum of crimes that occurred in a cuadrante or sector for a specified period of time.
    # /api/v1/cuadrantes/(string: cuadrante)/crimes/(string: crime)/series

    async def count_of_crimes_ocurred_in_cuadrante(self):
        """
        Return the count of crimes that occurred in a cuadrante, by d ate.
        2019-01 - 2020-12 funciona.
        2018-01 - 2020-12 funciona.
        2017-01 - 2020-12 funciona.
        2016-01 - 2020-12 funciona
        2015-12 - 2020-12 {'error': 'something is wrong with the start_date date you provided'}

        Creo que el 2016 es lo menos que se puede pedir.

        2016-01 - 2021-01: ....
        2016-01 - 2021-01: ....

        """
        # date_params = dict(
        #     start_date="2016-01",
        #     end_date="2021-01",
        # )
        response = await self.client.async_client.get('/api/v1/cuadrantes/all/crimes/all/period',  # params=date_params,
                                                      timeout=30000)
        return response.json()

    async def sum_of_crimes_occurred_in_sector(self):
        """
        Return the sum of crimes that occurred in a particular or in all sectores for a specified period of time.
        By default it returns the sum of crimes during the last 12 months for all the sectores in the database.
        """
        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/sectores/all/crimes/all/period',  # params=date_params,
                                                      timeout=30000)
        return response.json()

    # ============ TOP MOST VIOLENT ===============
    async def top_ranked_sectors_with_highest_crime_rates_for_period_of_time(self):
        """
        Return the top ranked sectors with the highest crime rates for a given period of time.
        When no date parameters are specified the top 5 cuadrantes are returned for the last 12 months
        (e.g. If July is the last date in the database then the period July 2018 to Aug 2017 will be analyzed).
        Crimes where no sector was specified (NO ESPECIFICADO) are ignored. All population data returned by this
        call is in persons/year and comes from the 2010 census
        """

        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/sectores/crimes/all/top/rates',  # params=date_params,
                                                      timeout=30000)
        return response.json()

    async def top_ranked_cuadrantes_with_highest_crime_counts_for_period_of_time(self):
        """
        Return the top ranked cuadrantes with the highest crime counts for a given period of time.
        When no dates parameters are specified the top 5 cuadrantes for the last 12 months are returned
        (e.g. If July is the last date in the database, then the period July 2018 to Aug 2017 will be analyzed).
        All population data returned by this call is in persons/year and comes from the 2010 census
        """

        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/cuadrantes/crimes/all/top/counts',  # params=date_params,
                                                      timeout=30000)
        return response.json()

    async def top_ranked_cuadrantes_where_crime_increased_most(self):
        """
        Return the top ranked cuadrantes where crime counts increased the most.
        When no date parameters are specified the top 5 cuadrantes are returned for the last 3 months compared with
        the same period during the previous year (e.g. July-May 2018 compared with July-May 2017).
         All population data returned by this call is in persons/year and comes from the 2010 census
        """

        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/cuadrantes/crimes/all/top/counts/change',
                                                      # params=date_params,
                                                      timeout=30000)
        return response.json()

    # ============ DF DATA ===============
    async def sum_crimes_in_df(self):
        """
        Return the sum of crimes that occurred in the Federal District
        """

        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/df/crimes/all/series',  # params=date_params,
                                                      timeout=30000)
        return response.json()

    # ============ ENUMERATE ===============

    async def lista_sectores(self):
        response = await self.client.async_client.get('/api/v1/sectores')
        return response.json()

    async def lista_cuadrantes(self):
        response = await self.client.async_client.get('/api/v1/cuadrantes')
        return response.json()

    async def lista_crimes(self):
        response = await self.client.async_client.get('/api/v1/crimes')
        return response.json()

    async def lista_municipios(self):
        response = await self.client.async_client.get('/api/v1/municipios')
        return response.json()

    async def change_in_crime_counts_for_period_at_cuadrante_level(self):
        """
        Return the change in crime counts for a specified period of time at the cuadrante level
        By default it returns the change during the last 12 months

        """
        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/cuadrantes/all/crimes/all/period/change',
                                                      # params=date_params,
                                                      timeout=30000)
        return response.json()

    # ============ GEOJSON ===============

    async def geojson_cuadrantes_delictivos(self):
        """
        Returns a map of the cuadrantes delictivos encoded as geojson.
        """
        response = await self.client.async_client.get('/api/v1/cuadrantes/geojson', timeout=30000)
        return response.json()

    async def geojson_sectores_delictivos(self):
        """
        Returns a map of the sectores delictivos encoded as geojson
        """
        response = await self.client.async_client.get('/api/v1/sectores/geojson', timeout=30000)
        return response.json()

    async def geojson_municipios_delictivos(self):
        """
        Returns a map of the municipios delictivos encoded as geojson
        """
        response = await self.client.async_client.get('/api/v1/municipios/geojson', timeout=30000)
        return response.json()

    # ============ DAY AND HOUR ===============
    async def crimes_by_hour(self):
        """
        Return the number of crimes by hour
        """
        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/df/crimes/all/hours',
                                                      # params=date_params,
                                                      timeout=30000)
        return response.json()

    async def crimes_by_days(self):
        """
        Return the number of crimes by days
        """
        # date_params = dict(
        #     start_date="2019-01",
        #     end_date="2019-03",
        # )
        response = await self.client.async_client.get('/api/v1/df/crimes/all/days',
                                                      # params=date_params,
                                                      timeout=30000)
        return response.json()
