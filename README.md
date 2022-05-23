# Youtube Data Pipeline

## O projeto realiza a automação da extração de dados das playlist dos canais da MARVEL BRASIL e do DISNEY+ BRASIL no youtube
### O mesmo apresenta pipelines de continuous integration e continuous delivery (CI/CD) através do github actions e testes automatizados com a biblioteca pytest
<div align="center">
<img width=200 src='https://img.icons8.com/plasticine/400/disney-plus.png'>
<img width=200 src='https://logospng.org/download/marvel/logo-marvel-1536.png'>
</div>

## Etapas da construção:
* Foi necessário a utilização da api do youtube para realizar a extração dos dados de algumas playlists dos canais 
* Contrução da aplicação em flask que realiza a coleta dos dados de cada vídeo da playlist
* Foram utilizados alguns serviços do google cloud para realizar a automação da extração
  - Google Cloud Scheduler -> alarme do processo automatizado
  - Google Cloud Run -> implantação da aplicação flask
  - Google Pubsub -> sistema de mensageria que realiza 
  - Google BigQuery -> armazenamento dos dados coletados
<div align="center">
<img src='https://miro.medium.com/max/1400/1*vfcEHNjLjWaG3O4OFwo-sQ.png'>
</div>

## É através dessa arquiterura que conseguimos carregar os dados dos vídeos automaticamente nas bases de dados do BigQuery


