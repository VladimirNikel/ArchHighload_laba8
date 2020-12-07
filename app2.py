import json
import datetime
import random
import time

print("*"*50,"\nНазвание: Приниматель запросов 3000.\nЭта программа умеет принимать сообщение из общего топика Kafka.\nЕсли сообщение типа *message*, то мы пишем, что всё ок, иначе - отправляем это сообщение в другой топик (dead_letter).\nАвтор: Ниемисто Владимир, Nikel 2020")
print("*"*50)

try:
	while 1:
		time.sleep(random.randint(0, 5))
		vybor = (random.randint(1, 1000))%3
		if vybor == 0:
			send_json = json.dumps({"type":"message"})
		else:
			if vybor == 1:
				send_json = json.dumps({"type":"error"})
			else:
				send_json = json.dumps({"type":"nikel"})
		

		if (json.loads(send_json))['type'] == 'message':
			print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\t",json.loads(send_json),"\tDone")
			send_json = json.dumps({"type":"message"})

		else:
			print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\t",json.loads(send_json),"\t -> в другой топит топай!")
			send_json = json.dumps({"type":"error"})
		
except KeyboardInterrupt:
	print("\n")
	print("*"*50,"\nДо встречи, бро!\nNikel, 2020 ")