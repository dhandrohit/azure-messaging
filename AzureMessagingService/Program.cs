// See https://aka.ms/new-console-template for more information

using Azure.Messaging.ServiceBus;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using AzureMessagingService;
using Newtonsoft.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

Dept d = new Dept()
{
    deptno = 10,
    dname = "Solution Architecture"
};
Dept d1 = new Dept()
{
    deptno = 20,
    dname = "Marketing"
};

////////////////////////////////////////////////
////////////////FOR AZURE SERVICE BUS - TOPICS
///
String connectionString = "Endpoint=sb://rkvservicebus.servicebus.windows.net/;SharedAccessKeyName=rkvpolicy;SharedAccessKey=7iUly3PpgMNloxMix0iCF87rF/IqyTtHr+ASbAx4mnk=;EntityPath=rkvtopic";
String topicName = "rkvtopic";
String subscriptionName = "rkvsubsa";
async Task sendMsgServiceBusTopic(Dept dept)
{
    ServiceBusClient client = new ServiceBusClient(connectionString);
    ServiceBusSender serviceBusSender = client.CreateSender(topicName);
    ServiceBusMessage message = new ServiceBusMessage(JsonConvert.SerializeObject(dept));
    message.ContentType = "application/json";
    message.MessageId = dept.deptno.ToString();
    //message.TimeToLive = TimeSpan.FromSeconds(10);
    //message.ApplicationProperties.Add("importance", "High");
    await serviceBusSender.SendMessageAsync(message);
    Console.WriteLine("Message Sent to Azure Service Bus - Topic");
}
//await sendMsgServiceBusTopic(d);
async Task receiveServiceBusTopicMsg()
{
    ServiceBusClient client = new ServiceBusClient(connectionString);
    ServiceBusReceiver serviceBusReceiver = client.CreateReceiver(topicName, subscriptionName, new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
    ServiceBusReceivedMessage message = await serviceBusReceiver.ReceiveMessageAsync();
    Dept dept = JsonConvert.DeserializeObject<Dept>(message.Body.ToString());
    Console.WriteLine($"Message Read : {dept.deptno} and {dept.dname}");
    //Console.WriteLine($"Message Properties: {message.ApplicationProperties["importance"].ToString()}");
    //await serviceBusReceiver.CompleteMessageAsync(message);
    Console.ReadLine();
}
await receiveServiceBusTopicMsg();




////////////////////////////////////////////////
////////////////FOR AZURE SERVICE BUS - QUEUE
///
//String connectionString = "Endpoint=sb://rkvservicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=w10YlRYOFmEeMSh63jKOoxovCEKr2hMBf+ASbDyTg2g=";
String queueName = "rkvqueue";

//async Task sendMsgServiceBusQueue(Dept dept)
//{
//    ServiceBusClient client = new ServiceBusClient(connectionString);
//    ServiceBusSender serviceBusSender = client.CreateSender(queueName);
//    ServiceBusMessage message = new ServiceBusMessage(JsonConvert.SerializeObject(dept));
//    message.ContentType= "application/json";
//    message.MessageId = dept.deptno.ToString();
//    //message.TimeToLive = TimeSpan.FromSeconds(10);
//    //message.ApplicationProperties.Add("importance", "High");
//    await serviceBusSender.SendMessageAsync(message);
//    Console.WriteLine("Message Sent to Azure Service Bus - Queue");
//}
//await sendMsgServiceBusQueue(d);
//async Task PeekAzureServiceBusMessage()
//{
//    String connectionString = "Endpoint=sb://rkvservicebus.servicebus.windows.net/;SharedAccessKeyName=rkvqueuepolicy;SharedAccessKey=KIX6RYw9T5FmuBd4Wj4bsAFKrzOoVH8zJ+ASbO8IXRw=;EntityPath=rkvqueue/$DeadLetterQueue";
//    String queueName = "rkvqueue/$DeadLetterQueue";
//    ServiceBusClient client = new ServiceBusClient(connectionString);
//    ServiceBusReceiverOptions serviceBusReceiverOptions = new ServiceBusReceiverOptions();
//    serviceBusReceiverOptions.ReceiveMode = ServiceBusReceiveMode.PeekLock;
//    ServiceBusReceiver serviceBusReceiver = client.CreateReceiver(queueName, serviceBusReceiverOptions);
//    ServiceBusReceivedMessage message = await serviceBusReceiver.ReceiveMessageAsync();
//    Dept dept = JsonConvert.DeserializeObject<Dept>(message.Body.ToString());
//    Console.WriteLine($"Message Read : {dept.deptno} and {dept.dname}");
//    Console.WriteLine($"Message Properties: {message.ApplicationProperties["importance"].ToString()}");
//    await serviceBusReceiver.CompleteMessageAsync(message); 
//    Console.ReadLine();    
//}
////await PeekAzureServiceBusMessage();
//async Task PeekAzureServiceBusMessages()
//{
//    ServiceBusClient client = new ServiceBusClient(connectionString);
//    ServiceBusReceiverOptions serviceBusReceiverOptions = new ServiceBusReceiverOptions();
//    serviceBusReceiverOptions.ReceiveMode = ServiceBusReceiveMode.PeekLock;
//    ServiceBusReceiver serviceBusReceiver = client.CreateReceiver(queueName, serviceBusReceiverOptions);
//    IAsyncEnumerable<ServiceBusReceivedMessage> messages = serviceBusReceiver.ReceiveMessagesAsync();
//    await foreach (ServiceBusReceivedMessage item in messages)
//    {  
//        Dept dept = JsonConvert.DeserializeObject<Dept>(item.Body.ToString());
//        Console.WriteLine($"Message Read : {dept.deptno} and {dept.dname}");
//        Console.WriteLine($"Message Properties: {item.ApplicationProperties["importance"].ToString()}");
//    }
//}
//await PeekAzureServiceBusMessages();

////////// ENDS HERE

///AZURE STORAGE QUEUE
String connString = "DefaultEndpointsProtocol=https;AccountName=rkvstorageacct;AccountKey=7nv7WyS3OXxlsLQhF7veC3rd1T4UYZFqfHgONmG8CL4YzYWffRoplEltqegMAxhGtVqlIr59A5Sb+AStmBnS+A==;EndpointSuffix=core.windows.net";
//sendMessage("Test Message 1");
//sendMessage("Test Message 2");
//sendMessageObjectWithBase64();
void sendMessage(String message)
{
    QueueClient client = new QueueClient(connString, queueName);
    client.SendMessage(message);
    Console.WriteLine("Message Sent!");
}
void sendMessageObject()
{
    Dept d = new Dept()
    {
        deptno = 20,
        dname = "Solution Architecture"
    };
    QueueClient client = new QueueClient(connString, queueName);
    client.SendMessage(JsonConvert.SerializeObject(d));
    Console.WriteLine("Message Sent!");
}
void sendMessageObjectWithBase64()
{
    Dept d = new Dept()
    {
        deptno = 20,
        dname = "Solution Architecture"
    };
    QueueClient client = new QueueClient(connString, queueName);
    var jsonObject = JsonConvert.SerializeObject(d);
    var bytes = System.Text.Encoding.UTF8.GetBytes(jsonObject);
    var message = System.Convert.ToBase64String(bytes);
    client.SendMessage(message);
    Console.WriteLine("Message Sent!");
}
void receiveMessageObject()
{
    int maxMessages = 5;
    QueueClient client = new QueueClient(connString, queueName);
    QueueMessage[] messages = client.ReceiveMessages(maxMessages);
    foreach (var item in messages)
    {
        Dept temp = JsonConvert.DeserializeObject<Dept>(item.Body.ToString());
        Console.WriteLine($"The Dept id is: {temp.deptno} and Message Body is {temp.dname}");
        client.DeleteMessage(item.MessageId, item.PopReceipt);
    }
}
void receiveMessage()
{
    int maxMessages = 5;
    QueueClient client = new QueueClient(connString, queueName);
    QueueMessage[] messages = client.ReceiveMessages(maxMessages);
    foreach (var item in messages)
    {
        Console.WriteLine($"The item id is: {item.MessageId} and Message Body is {item.Body}");
        client.DeleteMessage(item.MessageId, item.PopReceipt);
    }

}
void peekMessages()
{
    int maxMessages = 5;
    QueueClient client = new QueueClient(connString, queueName);
    PeekedMessage[] messages = client.PeekMessages(maxMessages);
    foreach (var item in messages)
    {
        Console.WriteLine($"The item id is: {item.MessageId} and Message Body is {item.Body}");
    }
    
}
// AZURE STORAGE QUEUE ENDS HERE......
