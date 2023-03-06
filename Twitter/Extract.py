import tweepy
from tweepy import Stream
import json
import pytz
from s3fs import S3FileSystem

# token
bearer = 'KEY da api do twitter aqui AQUI'

# aqui estou usando subclasse do StreamingCliente pra poder salvar os dados
class streaming_client(tweepy.StreamingClient):
    # esses atributos irão me auxiliar na hora de salvar os dados
    count = 0
    maxtweets = 100
    jsonlist = []
    date_format = '%Y-%m-%d %H:%M:%S'
    timezone = "America/Sao_Paulo"

    # função que executa sempre que receber um tweet do streaming
    def on_tweet(self, tweet):
        jso = {"id":tweet.id, "tweet_text": tweet.text, "tweet_date": tweet.created_at.astimezone(pytz.timezone(self.timezone)).strftime(self.date_format)}
        self.jsonlist.append(jso)
        self.count = self.count+1

        # esse trecho vai identificar se o programa ja tem a quantidade de dados necessaria para gerar um arquivo
        if(self.count == self.maxtweets):
            self.count = 0
            # estou salvando com hora, minuto E SEGUNDOS, pra não ter problema de sobrescricão
            firstdate = self.jsonlist[0]['tweet_date']
            firstdate = firstdate.replace(":","-")

            # aqui estou salvando direto no S3
            path = 's3://elian-xpto-raw/dados/'
            s3 = S3FileSystem(anon=False, key='KEY AQUI', secret='KEY AQUI')
            with s3.open(path + firstdate + '.json', 'w', encoding='utf-8') as file:
                json.dump(self.jsonlist, file, ensure_ascii=False)
            
            
            #aqui estou salvando na minha maquina
            # path = 'dados/'
            # with open(path + firstdate + '.json', 'w', encoding='utf-8') as f:
            #     json.dump(self.jsonlist, f, ensure_ascii=False)

            self.jsonlist = []

        print(tweet.id, tweet.text, tweet.created_at.astimezone(pytz.timezone(self.timezone)).strftime(self.date_format))

# instanciando a classe em uma variavel
printer = streaming_client(bearer)

# manipulando regras para filtrar o que eu quero receber pelo streaming
print(printer.get_rules())
printer.delete_rules([rule.id for rule in printer.get_rules()[0]])

# regras para buscar dados sobre o bolsonaro
printer.add_rules(tweepy.StreamRule("bolsonaro -is:retweet -is:reply -has:links -has:media lang:pt"))
printer.add_rules(tweepy.StreamRule("bozo -is:retweet -is:reply -has:links -has:media lang:pt"))
printer.add_rules(tweepy.StreamRule("biroliro -is:retweet -is:reply -has:links -has:media lang:pt"))
printer.add_rules(tweepy.StreamRule("bolsolixo -is:retweet -is:reply -has:links -has:media lang:pt"))

# regras para buscar dados sobre o lula
printer.add_rules(tweepy.StreamRule("lula -is:retweet -is:reply -has:links -has:media lang:pt"))
printer.add_rules(tweepy.StreamRule("lulinha -is:retweet -is:reply -has:links -has:media lang:pt"))
printer.add_rules(tweepy.StreamRule("(ex presidiario) -is:retweet -is:reply -has:links -has:media lang:pt"))

print(printer.get_rules())


printer.filter(tweet_fields = 'created_at')
