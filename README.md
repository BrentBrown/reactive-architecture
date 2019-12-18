# Reactive Architecture:
## Supervisor Consumer with Flow Control

### Run RabbitMQ
docker run -d --hostname my-rabbit --name some-rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management

### Without Flow Control
* Build and run 
  * /monitor/AMQPMonitor
  * /producerNoFlow/AMQPProducerNoFlow
  * /supervisor/AMQPSupervisor

### With Flow Control
* Build and run
  * /monitor/AMQPMonitor
  * /flowMonitor/AMQPFlowMonitor
  * /producerFlow/AMQPProducerFlow
  * /supervisor/AMQPSupervisor

