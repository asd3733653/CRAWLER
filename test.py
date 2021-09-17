import requests
import re

d = {"美金": "USD", "日元": "JPY", "韓元": "KRW", "人民幣": "CNY", "港幣": "HKD", "泰銖": "THB", "台幣": "TWD"}
t = "TWD"
quest = str("55美金")
jsonapi = requests.get('https://tw.rter.info/capi.php').json()
number = re.match('\d{1,}', quest).group()
print(quest.split(number)[1])
dollar_type = d["美金"]
# print(dollar_type)
print(jsonapi[f'{dollar_type}{t}']["Exrate"])
