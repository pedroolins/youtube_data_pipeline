from pickle import TRUE
from src.pega_dados import pega_comments_videos, pega_dados_da_playlist, pega_stats_videos
from src.pega_dados import pega_stats_channel, pega_tudo_playlist
import pandas as pd
from pytest import mark
# type(pega_comments_videos()["video_id"])
type(pega_tudo_playlist())

## testando as funções iniciais do arquivo pega_dados.py e vendo se retornam um json
@mark.dataframes
def test_pega_dados_comments_is_dataframe():
    df = pega_comments_videos()
    assert isinstance(df, pd.core.frame.DataFrame) == True

@mark.dataframes
def test_pega_comments_videos_has_6_columns():
    df = pega_comments_videos()
    assert df.shape[1] == 6

@mark.dataframes
def test_pega_stats_videos_is_dataframe():
    df = pega_stats_videos()
    assert isinstance(df, pd.core.frame.DataFrame) == True

@mark.dataframes
def test_pega_stats_videos_has_6_columns():
    df = pega_stats_videos()
    assert df.shape[1] == 6

@mark.dataframes
def test_pega_dados_playlist_is_dataframe():
    df = pega_dados_da_playlist()
    assert isinstance(df, pd.core.frame.DataFrame) == True

@mark.dataframes
def test_pega_dados_da_playlist_has_6_columns():
    df = pega_dados_da_playlist()
    assert df.shape[1] == 8

## testando a func que retorna em dataframe os dados dos canais
@mark.channels_dataframes
def test_pega_stats_channels():
    df = pega_stats_channel()
    assert isinstance(df, pd.core.frame.DataFrame) == True

@mark.channels_dataframes
def test_pega_stats_channels_has_6_columns():
    df = pega_stats_channel()
    assert df.shape[1] == 5

## testando a func que retorna pra gente as 3 tabelas com os dados da playlist que vamos subir no bq
@mark.tupla_final
def test_pega_dados_tudo_playlist_is_tuple():
    tuple_dfs = pega_tudo_playlist()
    assert isinstance(tuple_dfs, tuple) == True









