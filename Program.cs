using Confluent.Kafka;
using KafkaFlow;
using KafkaFlow.Admin.Dashboard;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services
    .AddKafka(kafka => kafka
        .AddCluster(cluster => cluster
            .WithBrokers(["localhost:9092"])
            .AddConsumer(consumer => consumer
                .Topic("kafka-flow-chat")
                .WithGroupId("kafka-flow-explorer")
                .WithWorkersCount(1)
                .WithBufferSize(10)
            )
            .EnableAdminMessages("kafka-flow.admin")
            .EnableTelemetry("kafka-flow.admin")
        ))
    .AddControllers();

var app = builder.Build();

app.MapControllers();
app.UseKafkaFlowDashboard();

var kafkaBus = app.Services.CreateKafkaBus();
await kafkaBus.StartAsync();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

// Add endpoint to put messages to Kafka
app.MapPost("/send", async (IProducer<string, string> producer, string message) =>
{
    var result = await producer.ProduceAsync("kafka-flow-chat", new Message<string, string>
    {
        Key = "key",
        Value = message
    });
    return Results.Ok(result);
});

await app.RunAsync();