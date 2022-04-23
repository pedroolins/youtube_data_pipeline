import googleapiclient.discovery
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import datetime
from pytz import timezone
import os

load_dotenv()

## config
KEY_API = os.getenv('chave_api_youtube')
SERVICE_NAME='youtube'
VERSION='v3'
## ids playlist and channels
ID_PLAYLIST_CAVALEIRO_DA_LUA = "PL4M4DVGB6vMLl0yokT_VpX_uLAEt_yBSt"
ID_PLAYLIST_MISS_MARVEL = "PL4M4DVGB6vMLJrIH93c3K3I8lasy1DQ9o"
ID_PLAYLIST_GAVIAO = "PL4M4DVGB6vMLTTjA1fIZfIW-sS5TCsKW9"
ID_PLAYLIST_DOCTOR_STRANGE = "PL4M4DVGB6vMJ7XsOssB5hLZP4LdqaFJMx"
ID_PLAYLIST_ETERNOS = "PL4M4DVGB6vMJP8KMJv65xCDvnmiRyKAPk"
ID_PLAYLIST_LOKI = "PL4M4DVGB6vMLPi7pkOFjHq-tBDUPZRuAh"
ID_PLAYLIST_VIUVA_NEGRA = "PL4M4DVGB6vMKkYpJEYxM4zSUOJKkZuaVN"
ID_PLAYLIST_FALCAO_SOLDADO = "PL4M4DVGB6vMLekuB-F-v1Vn5I136CVSxf"
ID_PLAYLIST_SANG_CHI = "PL4M4DVGB6vMIvUWrzOzL-d4WT1xmVZdxC"
ID_PLAYLIST_WHAT_IF = "PL4M4DVGB6vMIESCt3JOuHItJjV1L1-sJA"
ID_PLAYLIST_SPIDER_MAN_3 = "PL4M4DVGB6vMKFHKUm3aBTmNNIRoARvvlN"
ID_PLAYLIST_WANDA_VISION = "PL4M4DVGB6vMIbzvWMrFgtFeg6nvnl7Aym"


CHANNEL_ID_DISNEY_PLUS = 'UCApaSzvP6jM9rfs8UbcqD1g'
CHANNEL_ID_MARVEL = "UCItRs-h8YU1wRRfP637614w"
PLAYLIST_NAME = "miss marvel"
VIDEO_ID = 'kHhJlL4d5Vo'
# next_page_token = None

## instanciando
youtube = googleapiclient.discovery.build(
    serviceName=SERVICE_NAME, version=VERSION, developerKey=KEY_API
)

def pega_dados_totais(id_channel=CHANNEL_ID_DISNEY_PLUS):
    res = youtube.channels().list(part='statistics', id=id_channel).execute()
    return res

def pega_dados_playlist(pagetoken=None, id_playlist=ID_PLAYLIST_CAVALEIRO_DA_LUA, max_results=50):
    res = youtube.playlistItems().list(part=['snippet', 'contentDetails'], playlistId=id_playlist, maxResults=max_results, pageToken=pagetoken).execute()
    return res

def pega_dados_video(id_video=VIDEO_ID):
    res = youtube.videos().list(part='statistics', id=id_video, maxResults=50).execute()
    return res

def pega_json_comments(id_video=VIDEO_ID, max_results=50, pagetoken=None):
    res = youtube.commentThreads().list(part=['snippet', 'replies'], videoId=id_video,  maxResults=max_results, pageToken=pagetoken).execute() 
    return res

##pegando os dados da playlist
def pega_dados_da_playlist(id_playlist=ID_PLAYLIST_CAVALEIRO_DA_LUA, playlist_name="cavaleiro da lua"):
    playlist = []
    pagetoken = None
    while True:
        dados_json = pega_dados_playlist(id_playlist=id_playlist, pagetoken=pagetoken) 
        pagetoken = dados_json.get('nextPageToken')
        playlist += dados_json['items']
        if pagetoken is None:
            break
    # print(playlist)
    video_title_list = [videos['snippet']['title'] for videos in playlist]
    video_id_list = [videos['snippet']['resourceId']['videoId'] for videos in playlist]
    video_description_list = [videos['snippet']['description'] for videos in playlist]
    channel_video_list = [videos['snippet']['channelTitle'] for videos in playlist]
    video_playlist_id_list = [videos['snippet']['playlistId'] for videos in playlist]
    publication_date = [videos['contentDetails']['videoPublishedAt'][0:10] if 'videoPublishedAt' in videos['contentDetails'] else np.nan  for videos in playlist]
    dados_df = {
        'video_id': video_id_list,
        'channel_title': channel_video_list,
        'title': video_title_list,
        'description': video_description_list,
        'playlist_id': video_playlist_id_list,
        'publication_date': publication_date
    }
    df = pd.DataFrame(dados_df)
    df = df.query('title != "Private video"').copy()
    df['publication_date'] = pd.to_datetime(df['publication_date'])
    df['playlist_name'] = playlist_name
    fuso_horario = timezone('America/Sao_Paulo')
    df['loading_ts'] = datetime.datetime.now().astimezone(fuso_horario).strftime(format='%Y-%m-%d %H:%M:%S')
    df['loading_ts'] = df.loading_ts.astype('datetime64')
    return df

##pega estatísticas de uma lista de vídeos 
def pega_stats_videos(list_id_videos=pd.Series([VIDEO_ID])):
    # divide em partes pra deixar mais otimizado hehehehhe
    parts_divide = (list_id_videos.shape[0] // 50) + 1
    lista_parts = np.array_split(list_id_videos, parts_divide)
    dados_stats = []
    for lista in lista_parts:
        list_videos = pega_dados_video(id_video=list(lista))['items']
        for video in list_videos:
            stats = video['statistics']
            video_id = video['id']
            stats['video_id'] = video_id
            dados_stats.append(stats)
    tabela_stats = pd.DataFrame(dados_stats)
    tabela_stats['viewCount'] = tabela_stats['viewCount'].astype(int)
    tabela_stats['likeCount'] = tabela_stats['likeCount'].astype(int)
    tabela_stats['favoriteCount'] = tabela_stats['favoriteCount'].astype(int)
    tabela_stats['commentCount'] = tabela_stats['commentCount'].astype(int)
    fuso_horario = timezone('America/Sao_Paulo')
    tabela_stats['loading_ts'] = datetime.datetime.now().astimezone(fuso_horario).strftime(format='%Y-%m-%d %H:%M:%S')
    tabela_stats['loading_ts'] = tabela_stats.loading_ts.astype('datetime64')
    return tabela_stats

## pega os comentários de um vídeo
def video_comments(video_id ='SecT4r0-BqE'):
    dados = []
    next_page_token = None
    ## laço que me retorna para dados uma lista com json de cada comentário
    while True:
        json = pega_json_comments(id_video=video_id, pagetoken=next_page_token)
        # pega o page token para puxar a próxima página
        next_page_token = json.get('nextPageToken')
        dados_brutos = json['items']
        dados += dados_brutos
        if next_page_token is None:
            break
    ## lista que armazena os dados finais retirados do json
    lista_dados = []
    for comments in dados:
        video_id = comments['snippet']['topLevelComment']['snippet']['videoId']
        author_name = comments['snippet']['topLevelComment']['snippet']['authorDisplayName']
        author_img = comments['snippet']['topLevelComment']['snippet']['authorProfileImageUrl']
        like_counts = comments['snippet']['topLevelComment']['snippet']['likeCount']
        text = comments['snippet']['topLevelComment']['snippet']['textOriginal']
        dict_dados = {'video_id': video_id, 'author_img': author_img, 'author_name': author_name, 'like_counts': like_counts, 'text': text}
        lista_dados.append(dict_dados)
        ## verifica se o comentário teve respostas e pega as respostas dos comentários caso queira
        # if comments['snippet']['totalReplyCount'] > 0:
        #     for reply in comments['replies']['comments']:
        #         video_id = reply['snippet']['videoId']
        #         author_name = reply['snippet']['authorDisplayName']
        #         author_img = reply['snippet']['authorProfileImageUrl']
        #         like_counts = reply['snippet']['likeCount']
        #         text = reply['snippet']['textOriginal']
        #         dict_dados_reply = {'video_id': video_id, 'author_img': author_img, 'author_name': author_name, 'like_counts': like_counts, 'text': text}
        #         lista_dados.append(dict_dados_reply)
    return lista_dados

## pega os comentários de uma lista de vídeos
def pega_comments_videos(list_videos_id=[VIDEO_ID]):
    comments_list = []
    for video_id in list_videos_id:
        comments_list += video_comments(video_id)
    df = pd.DataFrame(comments_list)
    fuso_horario = timezone('America/Sao_Paulo')
    df['loading_ts'] = datetime.datetime.now().astimezone(fuso_horario).strftime(format='%Y-%m-%d %H:%M:%S')
    df['loading_ts'] = df.loading_ts.astype('datetime64')
    return df

## retorna tanto os dados dos vídeos, como stats e comments das playlists selecionada
def pega_tudo_playlist(id_playlist=ID_PLAYLIST_CAVALEIRO_DA_LUA, playlist_name="cavaleiro_da_lua"):
    tabela_videos = pega_dados_da_playlist(id_playlist, playlist_name)
    tabela_stats = pega_stats_videos(tabela_videos['video_id'])
    tabela_comments = pega_comments_videos(tabela_videos['video_id'])
    return tabela_videos, tabela_stats, tabela_comments

def pega_stats_channel(id_channel=CHANNEL_ID_DISNEY_PLUS, channel_name="disney_plus"):
    dict_dados = pega_dados_totais(id_channel=id_channel)['items'][0]['statistics']
    dict_dados['channel_name'] = channel_name
    df = pd.DataFrame([dict_dados])
    fuso_horario = timezone('America/Sao_Paulo')
    df['loading_ts'] = datetime.datetime.now().astimezone(fuso_horario).strftime(format='%Y-%m-%d %H:%M:%S')
    df = df[['channel_name', 'viewCount', 'subscriberCount', 'videoCount',  'loading_ts']]
    df = df.assign(
        loading_ts = df['loading_ts'].astype('datetime64'),
        viewCount = df['viewCount'].astype(int),
        subscriberCount = df['subscriberCount'].astype(int),
        videoCount = df['videoCount'].astype(int),
    )
    return df


if __name__ == "__main__":
    # print(pega_dados_totais())
    df1, df2, df3 = pega_tudo_playlist(id_playlist=ID_PLAYLIST_ETERNOS, playlist_name=PLAYLIST_NAME)
    print(df1.dtypes)
    print(df2.dtypes)
    print(df3.dtypes)