namespace KafkaConsumer
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Confluent.Kafka;
    using Confluent.Kafka.Serialization;

    public class Program
    {
        static void Main(string[] args)
        {
            var config = new Dictionary<string, object>
      {
          { "group.id", "sample-consumer" },
          { "bootstrap.servers", "localhost:9092" },
          { "enable.auto.commit", "false"}
      };
            int counter = 0;
            List<int> list = new List<int>();

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Subscribe(new string[] { "kafkatopic" });

                consumer.OnMessage += (_, msg) =>
                {

                    list.Add(Int32.Parse(msg.Value));
                    counter++;
                   
                    if (counter % 10 == 0)
                    {
                        var lastTen = list.Skip(Math.Max(0, list.Count() - 10));
                        int sumLastTen = 0;
                        foreach (int num in lastTen)
                        {
                            sumLastTen += num;
                        }

                        Console.WriteLine("[" + DateTime.UtcNow + "] " + sumLastTen/10);
                        sumLastTen = 0;
                    }

                    consumer.CommitAsync(msg);
                };

                while (true)
                {
                    consumer.Poll(100);
                }
            }
        }
    }
}