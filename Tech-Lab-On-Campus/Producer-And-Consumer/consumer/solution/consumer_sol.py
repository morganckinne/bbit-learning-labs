import pika
import os
#from consumer_interface import mqConsumerInterface

class mqConsumer:
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        
        self.messageBindingKey = binding_key
        self.messageQueue = queue_name
        self.messageExchange = exchange_name
       


        self.setupRMQConnection()


    def setupRMQConnection(self) -> None:   
        #rabbitMQ
        mqConsumer_Paramaters = pika.URLParameters(os.environ["AMQP_URL"])
        self.messageConnection = pika.BlockingConnection(parameters=mqConsumer_Paramaters)

        #establish channel
        self.channel = self.messageConnection.channel()

        # queue
        self.channel.queue_declare(queue= self.messageQueue)

        # exchange
        self.exchange = self.channel.exchange_declare(exchange= self.messageExchange)


        # binding key 
        self.channel.queue_bind(queue=self.messageQueue, 
                                  routing_key=self.messageBindingKey, 
                                  exchange=self.messageExchange,)
        
        # receive messages
        self.channel.basic_consume(self.messageQueue, self.on_message_callback, auto_ack=False)

        


    def on_message_callback(
        self, channel, method_frame, header_frame, body
        ) -> None:
        #message received
        self.channel.basic_ack(method_frame.delivery_tag, False)
        print(f" message received: {body}")



    def startConsuming(self) -> None:
        print("waiting for message")
        self.channel.start_consuming()

    def __del__(self) -> None:
        print(f"closing connection")
        self.channel.close()
        self.messageConnection.close()









