using KafkaMessaging.GenericProcessor.Models;
using KafkaMessaging.GenericProcessor.Producer;
using Microsoft.AspNetCore.Mvc;

namespace KafkaMessaging.GenericProcessor.Controllers;

/// <summary>
/// Unified messaging controller using single producer service
/// </summary>
[ApiController]
[Route("api/messages")]
public class MessagesController : ControllerBase
{
    private readonly ILogger<MessagesController> _logger;
    private readonly IMessageProducerService _producer;

    public MessagesController(
        ILogger<MessagesController> logger,
        IMessageProducerService producer)
    {
        _logger = logger;
        _producer = producer;
    }

    /// <summary>
    /// Create a new order
    /// </summary>
    [HttpPost("orders")]
    [ProducesResponseType(typeof(MessageProduceResult), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request, CancellationToken ct)
    {
        try
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var order = new OrderMessage
            {
                OrderId = Guid.NewGuid(),
                CustomerId = request.CustomerId,
                Amount = request.Items.Sum(i => i.UnitPrice * i.Quantity),
                OrderDate = DateTime.UtcNow,
                Items = request.Items
            };

            // Automatic topic resolution based on message type
            var result = await _producer.ProduceAsync(order, order.OrderId.ToString(), ct);

            _logger.LogInformation("Order {OrderId} published", order.OrderId);

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating order");
            return StatusCode(500, new { error = "Failed to create order", details = ex.Message });
        }
    }

    /// <summary>
    /// Process a payment
    /// </summary>
    [HttpPost("payments")]
    [ProducesResponseType(typeof(MessageProduceResult), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ProcessPayment([FromBody] CreatePaymentRequest request, CancellationToken ct)
    {
        try
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var payment = new PaymentMessage
            {
                PaymentId = Guid.NewGuid(),
                OrderId = request.OrderId,
                PaymentMethod = request.PaymentMethod,
                Amount = request.Amount,
                Status = "Pending",
                ProcessedAt = DateTime.UtcNow
            };

            // Automatic topic resolution
            var result = await _producer.ProduceAsync(payment, payment.PaymentId.ToString(), ct);

            _logger.LogInformation("Payment {PaymentId} published", payment.PaymentId);

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing payment");
            return StatusCode(500, new { error = "Failed to process payment", details = ex.Message });
        }
    }

    /// <summary>
    /// Send a notification
    /// </summary>
    [HttpPost("notifications")]
    [ProducesResponseType(typeof(MessageProduceResult), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> SendNotification([FromBody] CreateNotificationRequest request, CancellationToken ct)
    {
        try
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var notification = new NotificationMessage
            {
                NotificationId = Guid.NewGuid(),
                RecipientEmail = request.RecipientEmail,
                Subject = request.Subject,
                Body = request.Body,
                NotificationType = request.NotificationType
            };

            // Automatic topic resolution
            var result = await _producer.ProduceAsync(notification, notification.NotificationId.ToString(), ct);

            _logger.LogInformation("Notification {NotificationId} published", notification.NotificationId);

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error sending notification");
            return StatusCode(500, new { error = "Failed to send notification", details = ex.Message });
        }
    }

    /// <summary>
    /// Update inventory
    /// </summary>
    [HttpPost("inventory")]
    [ProducesResponseType(typeof(MessageProduceResult), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> UpdateInventory([FromBody] UpdateInventoryRequest request, CancellationToken ct)
    {
        try
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var update = new InventoryUpdateMessage
            {
                UpdateId = Guid.NewGuid(),
                ProductId = request.ProductId,
                QuantityChange = request.QuantityChange,
                NewQuantity = request.NewQuantity,
                Reason = request.Reason,
                UpdatedAt = DateTime.UtcNow
            };

            // Automatic topic resolution
            var result = await _producer.ProduceAsync(update, update.UpdateId.ToString(), ct);

            _logger.LogInformation("Inventory update {UpdateId} published", update.UpdateId);

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating inventory");
            return StatusCode(500, new { error = "Failed to update inventory", details = ex.Message });
        }
    }

    /// <summary>
    /// Update shipping status
    /// </summary>
    [HttpPost("shipping")]
    [ProducesResponseType(typeof(MessageProduceResult), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> UpdateShipping([FromBody] UpdateShippingRequest request, CancellationToken ct)
    {
        try
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            var shipping = new ShippingMessage
            {
                ShipmentId = Guid.NewGuid(),
                OrderId = request.OrderId,
                TrackingNumber = request.TrackingNumber,
                Carrier = request.Carrier,
                Status = request.Status,
                UpdatedAt = DateTime.UtcNow
            };

            // Automatic topic resolution
            var result = await _producer.ProduceAsync(shipping, shipping.ShipmentId.ToString(), ct);

            _logger.LogInformation("Shipping update {ShipmentId} published", shipping.ShipmentId);

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating shipping");
            return StatusCode(500, new { error = "Failed to update shipping", details = ex.Message });
        }
    }

    /// <summary>
    /// Produce a message to a specific topic (advanced usage)
    /// </summary>
    [HttpPost("custom/{topic}")]
    [ProducesResponseType(typeof(MessageProduceResult), StatusCodes.Status200OK)]
    public async Task<IActionResult> ProduceToCustomTopic(
        string topic,
        [FromBody] object message,
        [FromQuery] string? key = null,
        CancellationToken ct = default)
    {
        try
        {
            // This is for advanced scenarios where you want to specify the topic explicitly
            // For type safety, it's better to use the specific endpoints above
            
            _logger.LogWarning("Using custom topic endpoint - consider using typed endpoints instead");
            
            // You would need to implement custom logic here based on your requirements
            return BadRequest("Use typed endpoints for better type safety");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error producing to custom topic");
            return StatusCode(500, new { error = "Failed to produce message" });
        }
    }
}

// Request DTOs
public class CreateOrderRequest
{
    public string CustomerId { get; set; } = string.Empty;
    public List<OrderItem> Items { get; set; } = new();
}

public class CreatePaymentRequest
{
    public Guid OrderId { get; set; }
    public string PaymentMethod { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class CreateNotificationRequest
{
    public string RecipientEmail { get; set; } = string.Empty;
    public string Subject { get; set; } = string.Empty;
    public string Body { get; set; } = string.Empty;
    public string NotificationType { get; set; } = "Email";
}

public class UpdateInventoryRequest
{
    public string ProductId { get; set; } = string.Empty;
    public int QuantityChange { get; set; }
    public int NewQuantity { get; set; }
    public string Reason { get; set; } = string.Empty;
}

public class UpdateShippingRequest
{
    public Guid OrderId { get; set; }
    public string TrackingNumber { get; set; } = string.Empty;
    public string Carrier { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
}
