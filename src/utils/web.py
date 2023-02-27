import typing
import requests
from pathlib import Path

def download_dados_web(caminho: typing.Union[str, Path], url) -> None:
    """Realiza o download dos dados em um link da web

    Args:
        caminho: caminho para extração dos dados
        url: endereço do site a ser baixado
    """
    r = requests.get(url, stream=True)
    with open(caminho, 'wb') as arq:
        arq.write(r.content)