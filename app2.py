import json
import datetime
import random
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer('main_topic', group_id='test-consumer-group', bootstrap_servers=['194.61.2.84:5099'], api_version=(0,10,1), auto_offset_reset='earliest')

print("*"*50,"\nНазвание: Приниматель запросов 3000.\nЭта программа умеет принимать сообщение из общего топика Kafka.\nЕсли сообщение типа *message*, то мы пишем, что всё ок, иначе - отправляем это сообщение в другой топик (dead_letter).\nАвтор: Ниемисто Владимир, Nikel 2020")
print("*"*50)

try:
	while 1:
		"""
		time.sleep(random.randint(0, 5))
		vybor = (random.randint(1, 1000))%3
		if vybor == 0:
			send_json = json.dumps({"type":"message"})
		else:
			if vybor == 1:
				send_json = json.dumps({"type":"error"})
			else:
				send_json = json.dumps({"type":"nikel"})
		"""
		for msg in consumer:
			print(msg)

			if (json.loads(msg))['type'] == 'message':
				print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tDone")

			else:
				print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\t -> в другой топит топай!")

		
except KeyboardInterrupt:
	print("\n")
	print("*"*50,"\nДо встречи, бро!\nNikel, 2020 ")