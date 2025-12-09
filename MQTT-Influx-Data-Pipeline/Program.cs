using InfluxDB.Client;
using MQTT_Influx_Data_Pipeline.MQTT;

class Program
{
    const string MQTT_BROKER_TCP = "127.0.0.1";      // or "localhost"
    const int MQTT_BROKER_PORT = 1883;
    const string TOPIC = "test/topic";

    const string INFLUX_URL = "http://localhost:8086";
    const string INFLUX_TOKEN = "HmR9GF2MozEk31D5v0wyd4ctfRlIajeXYNBC4V7rOJTwu96WWWMnCjGyL_ZglWbZBuaKHyz0YyYJ_E-BNa_nwg==";
    const string INFLUX_ORG = "myorg";
    const string INFLUX_BUCKET = "mybucket";

    static async Task Main(string[] args)
    {
        Console.WriteLine("Starting MQTT -> Influx bridge...");

        // --- InfluxDB client ---
        using var influx = new InfluxDBClient(INFLUX_URL, INFLUX_TOKEN);
        var writeApi = influx.GetWriteApiAsync();

        // --- MQTT client ---
        var mqttClient = new MQTTClient(influx, MQTT_BROKER_TCP, MQTT_BROKER_PORT);

        // --- Open MQTT connection ---
        await mqttClient.ConnectAsync();

        // --- Subscribe to topic ---
        await mqttClient.SubscribeAsync(TOPIC);

        // --- Set message handler to store data into InfluxDB ---
        mqttClient.SetApplicationMessageReceivedHandlerToStoreIntoInflux(INFLUX_BUCKET, INFLUX_ORG);

        //// Send random data every 5 seconds (for testing)
        var cts = new CancellationTokenSource();
        _ = Task.Run(async () =>
        {
            var rnd = new Random();
            while (!cts.Token.IsCancellationRequested)
            {
                var payload = (20 + rnd.NextDouble() * 10).ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);

                await mqttClient.PublishMessage(TOPIC, payload);

                await Task.Delay(5000, cts.Token);
            }
        }, cts.Token);

        Console.WriteLine("Press Ctrl+C to exit.");
        Console.CancelKeyPress += (s, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        // Wait until cancellation
        while (!cts.IsCancellationRequested)
        {
            await Task.Delay(200);
        }

        // Let's wrap up
        await mqttClient.DisconnectAsync();
        mqttClient.Dispose();
        influx.Dispose();
    }
}