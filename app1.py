import json
import datetime
from kafka import KafkaProducer
import kafka

#security_protocol="SSL", 
#producer = KafkaProducer(bootstrap_servers=['194.61.2.84:2181'])				# value_serializer=lambda v: json.dumps(v).encode('utf-8')
producer = KafkaProducer(bootstrap_servers=['194.61.2.84:32769'], api_version=(0,10,1))
clustermetadate = kafka.cluster.ClusterMetadata(bootstrap_servers=['194.61.2.84:32769'])

print("*"*50,"\nНазвание: Посылатель запросов 3000.\nЭта программа умеет слать сообщение в общий топик Kafka.\nДля этого Вам будет предложено выбрать два имеющихся типа сообщений, но и также, предоставляется возможность ввести свой тип сообщений и отправить его в топик.\nАвтор: Ниемисто Владимир, Nikel 2020")
print("*"*50)

if producer.bootstrap_connected():
	print("Всё хорошо. Подключиться удалось... едем дальше:")

	print("Брокеры: ", clustermetadate.brokers())
	print("Известные топики: ", clustermetadate.topics())
	print("Установлено ли соединение: ", clustermetadate.partitions_for_topic('main_topic'))

	try:
		while 1:
			send_json = None
			try:
				send_type_message = int(input("\nКакой тип сообщения сейчас отправим?\n\t1 - 'message'\n\t2 - 'error'\n"))
			except ValueError:
				continue

			if send_type_message == 1:
				print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tОкей, пошлю сообщение типа *message*")
				send_json = (json.dumps({"type": "message"})).encode('utf-8')
				
			else:
				if send_type_message == 2:
					print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tОкей, пошлю сообщение типа *error*")
					send_json = (json.dumps({"type": "error"})).encode('utf-8')

				else:
					other_type_message = str(input("Вероятно, Вы хотите ввести свой тип сообщения? Введите его сейчас: "))
					print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),("\tОкей, тогда я отправлю сообщение типа *{}*".format(other_type_message)))
					send_json = (json.dumps({"type": other_type_message})).encode('utf-8')

			print(json.loads(send_json.decode('utf-8')))
			producer.send('main_topic', send_json)
			
	except KeyboardInterrupt:
		print("\n")
		print("*"*50,"\nДо встречи, бро!\nNikel, 2020 ")


else:
	print("\nСожалею, но до брокера не достучаться, видимо он занят или отдыхает... Попробуй позже, а я пока спать.\n")
	print("*"*50,"\nДо встречи, бро!\nNikel, 2020 ")

