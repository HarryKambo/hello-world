using KafkaMessaging.GenericProcessor;
using KafkaMessaging.GenericProcessor.Models;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new Microsoft.OpenApi.Models.OpenApiInfo
    {
        Title = "Kafka Messaging API",
        Version = "v1",
        Description = "API for producing and consuming messages with Kafka using Generic Message Processor"
    });
});

// ============================================================================
// KAFKA PRODUCER - Unified service (Recommended)
// ============================================================================
// Single producer service that handles all message types
builder.Services.AddMessageProducerService(builder.Configuration);

// ============================================================================
// KAFKA CONSUMER - Generic consumer with automatic handler resolution
// ============================================================================
builder.Services.AddGenericKafkaConsumer(builder.Configuration, topics =>
{
    // Handlers are automatically resolved from DI based on message type
    topics.AddOrdersTopic<OrderMessage>();
    topics.AddPaymentsTopic<PaymentMessage>();
    topics.AddNotificationsTopic<NotificationMessage>();
    topics.AddInventoryUpdatesTopic<InventoryUpdateMessage>();
    topics.AddShippingTopic<ShippingMessage>();
});

var app = builder.Build();

// Configure the HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Kafka Messaging API v1");
    });
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
