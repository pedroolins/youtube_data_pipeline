from flask import Flask, request
from dotenv import load_dotenv
import google.auth
import logging
import base64
from retrying import retry

from src.pega_dados import pega_stats_channel, pega_tudo_playlist
# from src.send_msg import print

load_dotenv()

gunicorn_logger = logging.getLogger("gunicorn.error")

credentials, project_id = google.auth.default()
print(credentials, project_id)

app = Flask(__name__)

app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)
level = gunicorn_logger.level
format = "%(levelname)8s (%(name)s:%(lineno)s): %(message)s"
logging.basicConfig(
    level=level,
    format=format,
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger(__name__)

@retry(stop_max_attempt_number=5, wait_fixed=3000)
def sobe_dados_bq(df, tabela_destino, mode):
    df.to_gbq(
        destination_table=f'youtube.{tabela_destino}',
        credentials=credentials,
        project_id=project_id,
        if_exists=mode
    )

## ids playlist e channels
ID_PLAYLIST_CAVALEIRO_DA_LUA = "PLCG86DHec6YGDUIHqtDT8IJzyCh8gJu6l"
ID_PLAYLIST_MS_MARVEL = "PL4M4DVGB6vMLJrIH93c3K3I8lasy1DQ9o"
ID_PLAYLIST_HAWKEYE = "PL4M4DVGB6vMLTTjA1fIZfIW-sS5TCsKW9"
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

## depara de playlist e channel id
DEPARA_PLAYLIST = {
    'disney_plus': CHANNEL_ID_DISNEY_PLUS,
    'marvel': CHANNEL_ID_MARVEL,
    'eternos': ID_PLAYLIST_ETERNOS, 
    'loki': ID_PLAYLIST_LOKI,
    'doutor estranho': ID_PLAYLIST_DOCTOR_STRANGE,
    'ms marvel': ID_PLAYLIST_MS_MARVEL,
    "cavaleiro da lua": ID_PLAYLIST_CAVALEIRO_DA_LUA,
    "homem aranha 3": ID_PLAYLIST_SPIDER_MAN_3,
    "shang chi": ID_PLAYLIST_SANG_CHI,
    'what if': ID_PLAYLIST_WHAT_IF,
    'wanda vision': ID_PLAYLIST_WANDA_VISION,
    'hawkeye': ID_PLAYLIST_HAWKEYE,
    'falcao e soldado invernal': ID_PLAYLIST_FALCAO_SOLDADO,
    'viuva negra': ID_PLAYLIST_VIUVA_NEGRA
}

@app.route('/', methods=["POST"])
def index():
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]

    name = "nada de playlist"
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        name = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()

    ## aqui começa o código de atualização acima é apenas verificação da msg pubsub 
    playlist_name = name
    log.info(f"recebeu do pubsub a msg: {playlist_name}")
    print(playlist_name)
    playlist_id = DEPARA_PLAYLIST.get(playlist_name)
    # print(playlist_id)
    if playlist_id:
        try:
            if playlist_name == "disney_plus" or playlist_name == "marvel":
                df = pega_stats_channel(id_channel=playlist_id, channel_name=playlist_name)
                df.to_gbq(
                    destination_table='youtube.channel_stats',
                    credentials=credentials,
                    project_id=project_id,
                    if_exists='append')
                print(
                    f'''
                        youtube_api:
                        foram adicionadas {df.shape[0]} do canal {playlist_name} na tabela channel_stats
                        '''
                    )
                return 'ok', 200

            ##caso nao seja um canal
            df_videos, df_stats, df_comments = pega_tudo_playlist(id_playlist=playlist_id, playlist_name=playlist_name)
            if df_videos.empty or df_stats.empty or df_comments.empty:
                msg = f'youtube_api: algumas tabelas da playlist {playlist_name} estão sem dados'
                print(msg)
                return 'ok', 200

            #subindo dados da tabela de vídeos
            sobe_dados_bq(df_videos, tabela_destino='videos', mode='append')
            log.info(f"subiu tabela videos da playlist {playlist_name}")
            
            #subindo dados da tabela de comments
            sobe_dados_bq(df_comments, tabela_destino='video_comments', mode='replace')
            log.info(f"subiu tabela comments da playlist {playlist_name}")

            #subindo dados da tabela de stats
            sobe_dados_bq(df_stats, tabela_destino='video_stats', mode='append')
            log.info(f"subiu tabela stats da playlist {playlist_name}")
            
            msg = f"youtube_api: foram adicionadas as 3 tabelas da playlist {playlist_name}"
            print(msg)
            return 'ok', 200
        
        except Exception as e:
            msg = f"youtube_api: Ocorreu um erro nas tabelas da playlist {playlist_name}, error: {e}"
            print(msg)
            return 'ok', 200
    else:
        msg = f'youtube_api: A playlist ou canal {playlist_name} não foi encontrada'
        print(msg)
        return 'ok', 200

@app.route('/teste', methods=['POST'])
def teste():
    json = request.get_json()
    if not json:
        return 'not ok', 404
    playlist_name = json.get('msg')
    print(playlist_name)
    playlist_id = DEPARA_PLAYLIST.get(playlist_name)
    print(playlist_id)
    if playlist_id:
        try:
            df_videos, df_stats, df_comments = pega_tudo_playlist(id_playlist=playlist_id, playlist_name=playlist_name)
            if df_videos.empty or df_stats.empty or df_comments.empty:
                msg = f'youtube_api: algumas tabelas da playlist {playlist_name} estão sem dados'
                print(msg)
                print(msg)
                return 'ok', 200
            df_comments.to_gbq(
                destination_table='youtube.video_comments',
                credentials=credentials,
                project_id=project_id,
                if_exists='append')

            df_videos.to_gbq(
                destination_table='youtube.videos',
                credentials=credentials,
                project_id=project_id,
                if_exists='append')

            df_stats.to_gbq(
                destination_table='youtube.video_stats',
                credentials=credentials,
                project_id=project_id,
                if_exists='append')

            msg = f"youtube_api: foram adicionadas as 3 tabelas da playlist {playlist_name}"
            print(msg)
            print(msg)
            return 'ok', 200
        except Exception as e:
            msg = f"youtube_api: Ocorreu um erro nas tabelas da playlist {playlist_name}"
            print(msg)
            print(msg)
            return 'ok', 200
    else:
        msg = f'youtube_api: playlist {playlist_name} não encontrada'
        print(msg)
        print(msg)
        return 'ok', 200
if __name__ == "__main__":
    app.run(host='0.0.0.0', port='8080')
