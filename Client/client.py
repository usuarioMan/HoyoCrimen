from httpx import AsyncClient


class Client:
    """
    Singleton Client.
    """
    __client = None

    def __new__(cls) -> "Client":
        if cls.__client is None:
            cls.__client = object.__new__(cls)
            cls.__client.async_client: AsyncClient = AsyncClient(base_url="https://hoyodecrimen.com")

        return cls.__client


def get_client() -> Client:
    """
    Returns the GlobalSession instance.
    :return: GlobalSession
    """
    return Client()
