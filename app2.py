import json
import datetime
import random
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
import os

consumer = KafkaConsumer('main_topic', 	bootstrap_servers=['194.61.2.84:32769'], api_version=(0,10,1), auto_offset_reset='earliest')
producer = KafkaProducer(				bootstrap_servers=['194.61.2.84:32769'], api_version=(0,10,1))

print("*"*50,"\nНазвание: Приниматель запросов 3000.\nЭта программа умеет принимать сообщение из общего топика Kafka.\nЕсли сообщение типа *message*, то мы пишем, что всё ок, иначе - отправляем это сообщение в другой топик (dead_letter).\nАвтор: Ниемисто Владимир, Nikel 2020")
print("*"*50)

try:
	last_offset = int(open("last_offset.txt", "r").read())
except FileNotFoundError:
	last_offset = 0
try:
	while 1:
		for msg in consumer:
			if msg.offset > last_offset:
				type_message = (json.loads(msg.value.decode('utf-8')))['type']
				if type_message == 'message':
					print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tDone")

				else:
					print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\t -> в другой топик топай!")
					send_json = (json.dumps({"type": type_message})).encode('utf-8')
					producer.send('dead_letter', send_json)

				last_offset = msg.offset
		
except KeyboardInterrupt:
	file = open("last_offset.txt", "w")
	file.write(str(last_offset))
	file.close()
	print("\n")
	print("*"*50,"\nДо встречи, бро!\nNikel, 2020 ")