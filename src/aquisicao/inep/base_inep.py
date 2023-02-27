import abc
from pathlib import Path
import typing
import pandas as pd
from src.aquisicao.base_etl import BaseETL
import os
import urllib
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from utils.web import download_dados_web


class BaseINEPETL(abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do INEP
    """

    URL: str = (
        "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/"
    )

    def __init__(
        self, entrada: str, saida: str, base, criar_caminho: bool = True
    ) -> None:
        """
        Instancia objeto de ETL do INEP

        Args:
            entrada (str): string com o caminho para a pasta de entrada
            saida (str): string com o caminho para a pasta de saida
            base (str): Nome da base quer vai para a URL do INEP
            criar_caminho (bool, optional): flag indicando se devemos criar os caminhos. Defaults to True.
        """
        super().__init__(entrada, saida, criar_caminho)

        self._url = f"{self.URL}/{base}"

    def le_pagina_inep(self) -> typing.Dict[str, str]:
        """Realiza o web-scraping da página de dados do INEP

        Returns: dicionário com o nome do arquivo e link para a página
        """
        html = urllib.request.urlopen(self.url).read()
        soup = BeautifulSoup(html, features="lxml")
        return {
            tag["href"].split("_")[-1]: tag["href"]
            for tag in soup.find_all("a", {"class": "external-link"})
        }

    def dicionario_para_baixar(self) -> typing.Dict[str, str]:
        """Lê os conteúdos da pasta de dados e seleciona apenas os arquivos
        a serem baixados como complementares

        Returns: dicionário com o nome do arquivo e link para a página
        """
        para_baixar = self.le_pagina_inep()
        baixados = os.listdir(str(self.caminho_entrada))
        return {arq: link for arq, link in para_baixar.items() if arq not in baixados}
    
    def dowload_conteudo(self) -> None:
        """Realiza o download dos dados INEP para uma pasta local
        """
        for arq, link in self.dicionario_para_baixar():
            caminho_arq = self.caminho_saida / arq
            download_dados_web(caminho_arq, link)
    
    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extrai os dados do objeto
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saída de interesse
        """
        pass

