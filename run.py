import click
from src.aquisicao.opcoes import EnumETL
from src.aquisicao.opcoes import ETL_DICT
import src.configs as conf_geral


@click.group()
def cli():
    pass


@cli.group()
def aquisicao():
    """
    Grupo de comandos que executam as funções de aquisição
    """
    pass


@aquisicao.command()
@click.option(
    "--etl",
    type=click.Choice([s.value for s in EnumETL]), help="Nome do ETL a ser executado",
    )
@click.option(
    "--entrada",
    default=conf_geral.PASTA_DADOS,
    help="String com o caminho para a pasta de entrada",
)
@click.option(
    "--saida",
    default=conf_geral.PASTA_SAIDA_AQUISICAO,
    help="String com o caminho para a pasta de saída",
)
@click.option(
    "--criar_caminho",
    default=True,
    help="Flag indicando se devemos criar os caminhos. Defaults to True.",
)
def processa_dado(etl: str, entrada: str, saida: str, criar_caminho: bool) -> None:
    """Executa o pipeline de ETL de uma determinada fonte

    Args:
        etl (str): nome do ETL a ser executado
        entrada (str): string com o caminho para a pasta de entrada
        saida (str): string com o caminho para a pasta de saida
        criar_caminho (bool, optional): flag indicando se devemos criar os caminhos. Defaults to True.
    """
    objeto = ETL_DICT[EnumETL(etl)](entrada, saida, criar_caminho)
    objeto.pipeline()


if __name__ == "__main__":
    cli()
