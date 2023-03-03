import abc
import typing
import os
import urllib
import zipfile
import pandas as pd

from bs4 import BeautifulSoup

from src.aquisicao.inep.base_inep import BaseINEPETL
from src.utils.web import download_dados_web


class BaseCensoEscolarETL(BaseINEPETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do Censo Escolar
    """

    _tabela: str

    def __init__(
        self, entrada: str, saida: str, tabela: str, criar_caminho: bool = True
    ) -> None:
        """
        Instancia objeto de ETL do Censo Escolar

        Args:
            entrada (str): string com o caminho para a pasta de entrada
            saida (str): string com o caminho para a pasta de saida
            tabela (str): Tabela do censo escolar a ser processada
            criar_caminho (bool, optional): flag indicando se devemos criar os caminhos. Defaults to True.
        """
        super().__init__(entrada, saida, "censo-escolar", criar_caminho)

        self._tabela = tabela

    def extract(self) -> None:
        """
        Extrai os dados do objeto
        """
        # Realiza o download dos dados do Censo
        self.download_conteudo()

        # Carrega as tabelas de interesse
        for arq in self.le_pagina_inep():
            with zipfile.ZipFile(arq) as arq_zip:
                nome_zip = [arq for arq in arq_zip.namelist() if self._tabela in arq][0]
                self._dados_entrada[arq] = pd.read_csv(
                    arq_zip.open(nome_zip), encoding="latin-1", sep=";"
                )

    # @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de saída de interesse
        """
        pass
