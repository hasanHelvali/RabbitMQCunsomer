using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitMQ_Cunsomer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.Uri = new Uri("amqp://guest:guest@localhost:5672/");
            using IConnection connection = connectionFactory.CreateConnection();
            using IModel channel = connection.CreateModel();

            #region P2P Tasarımı
            string queueName = "example-p2p-queue";
            
            channel.QueueDeclare(
                queue:queueName,
                durable:false,
                exclusive:false,
                autoDelete:false);

            EventingBasicConsumer cunsomer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: queueName, autoAck: false, cunsomer);
            
            cunsomer.Received += (sender, e) =>
            {
                Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));
            };

            Console.ReadLine();
            #endregion


            #region Publish/Subscribe Tasarımı
            string exchangeName = "example-pub-sub-exchange";

            channel.ExchangeDeclare(
                exchange: exchangeName,
                type: ExchangeType.Fanout);

            string queueName = channel.QueueDeclare().QueueName;
            
            channel.QueueBind(
               queue: queueName,
               exchange: exchangeName,
               routingKey: string.Empty);

            channel.BasicQos(
                prefetchCount: 1,
                prefetchSize: 0,
                global: false);
            /*Buradaki BasicQos fonksiyonu olceklendirme icindir. Olceklendirme
            istenmiyorsa yazılmasına gerek yoktur.*/

            EventingBasicConsumer cunsomer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: queueName, autoAck: true, cunsomer);

            cunsomer.Received += (sender, e) =>
            {
                string message = Encoding.UTF8.GetString(e.Body.Span);
                Console.WriteLine(message);
            };

            #endregion

            #region Work Queue Tasarımı
            string queueName = "example-work-queue";

            channel.QueueDeclare(
                queue: queueName,
                durable: false,
                exclusive: false,
                autoDelete: false);

            EventingBasicConsumer cunsomer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: queueName, autoAck: true, cunsomer);

            channel.BasicQos(
                prefetchCount:1,
                prefetchSize:0, 
                global: false);
            
            cunsomer.Received += (sender, e) =>
            {
                string message = Encoding.UTF8.GetString(e.Body.Span);
                Console.WriteLine(message);
            };
            #endregion

            #region Request/Response Tasarımı
            string requestQueueName = "example-request-response-queue";

            channel.QueueDeclare(
                queue: requestQueueName,
                durable: false,
                exclusive: false,
                autoDelete: false);

            EventingBasicConsumer cunsomer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: requestQueueName, autoAck: true, cunsomer);
            cunsomer.Received += (sender, e) =>
            {
                string message = Encoding.UTF8.GetString(e.Body.Span);
                Console.WriteLine($"Response {message}");
                //mesaj islendi. Response ı donelim.

                byte[] responseMessage = Encoding.UTF8.GetBytes($"Islem Tamamlandı : {message}");
                IBasicProperties basicProperties = channel.CreateBasicProperties();
                basicProperties.CorrelationId = e.BasicProperties.CorrelationId;
                channel.BasicPublish(
                    exchange:string.Empty,
                    routingKey:e.BasicProperties.ReplyTo,
                    basicProperties:basicProperties,
                    body:responseMessage);
            };
            #endregion
            Console.Read();
        }
    }
}
