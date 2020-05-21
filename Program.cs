using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading.Tasks;

namespace MyAzServiceBus
{
    class Program
    {
        private static int NUM_MESSAGES = 10;

        static async Task Main(string[] args)
        {
            var queueConnectionString = @"Endpoint=sb://didiservicebusns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=iqBJk+o7lKpCtOTZJGaqC4J7blJsGgWqtXKE10JKAgE=";
            var queueName = "didiMessagingQueue";
            var queueClient = new QueueClient(queueConnectionString, queueName);

            RegisterMessageReciever(queueClient, queueName);
            await SendMessages(queueClient, queueName);

            await Task.Delay(TimeSpan.FromMinutes(15));
            Console.WriteLine("Closing client and exiting now!");
            await queueClient.CloseAsync();
        }

        private static void RegisterMessageReciever(QueueClient queueClient, string queueName)
        {
            var messageHandlerOptions = new MessageHandlerOptions(async args =>
            {
                Console.WriteLine($"Queue Message Handler Exception thrown\n ${args.Exception}");
                await Task.CompletedTask;
            })
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
            };

            queueClient.RegisterMessageHandler(async (message, token) =>
            {
                HandleRecievedMessage(message);
                await queueClient.CompleteAsync(message.SystemProperties.LockToken);
            }, messageHandlerOptions);
        }

        private static void HandleRecievedMessage(Message recievedMessage)
        {
            var messageBytes = recievedMessage.Body;
            var message = Encoding.UTF8.GetString(messageBytes);

            Console.WriteLine($"Received Message: ${message}");
        }

        private static async Task SendMessages(QueueClient queueClient, string queueName)
        {
            Console.WriteLine($"Begin send messages to - ${queueName}");

            try
            {
                for (int i = 0; i < NUM_MESSAGES; i++)
                {
                    var messageBody = $"Test message {i} - {queueName} - {DateTimeOffset.Now}";
                    var messageBytes = Encoding.UTF8.GetBytes(messageBody);
                    var message = new Message(messageBytes);
                    
                    await queueClient.SendAsync(message);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            Console.WriteLine($"All messages sent to - {queueName}");
        }
    }
}
