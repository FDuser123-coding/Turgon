package dev.turgon.connector.stripe;

import com.fasterxml.jackson.databind.JsonNode;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.EndpointConfig;
import java.net.URI;

/**
 * An endpoint's configuration (the connection's {@code config}), all optional:
 *
 * <pre>
 * apiVersion: 2026-06-30.basil   # sent as Stripe-Version; default: the account's
 * url: https://api.stripe.com    # tests point it at a fake
 * pageSize: 100                  # events per page, at most 100
 * invoice:
 *   collectionMethod: send_invoice   # or charge_automatically
 *   daysUntilDue: 30                 # for send_invoice
 *   finalize: true                   # finalize after creating (assigns its number)
 * link:
 *   metadataKey: erp_payment_id      # the invoice metadata link-invoice sets
 * webhook:                           # Invoice.Paid also arrives from a webhook endpoint
 *   secretRef: openbao://stripe-invoicing/webhook-secret   # its signing secret (whsec_...)
 *   toleranceSeconds: 300            # how old a signature may be
 * </pre>
 *
 * The secret is a restricted key ({@code rk_live_...}) with read access to
 * customers and events and write access to invoices, or a secret key.
 */
public record StripeConfig(URI url, String apiVersion, int pageSize, String collectionMethod, int daysUntilDue, boolean finalizeInvoices,
        String metadataKey, byte[] webhookSecret, long toleranceSeconds, String webhookError) {

    public static StripeConfig from(EndpointConfig e) throws ConnectorException.Invalid {
        JsonNode c = e.config();
        String u = c.path("url").asText("https://api.stripe.com").replaceAll("/+$", "");
        URI url;
        try {
            url = URI.create(u);
            if (url.getScheme() == null || url.getHost() == null) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException x) {
            throw new ConnectorException.Invalid("url: not an absolute URL");
        }
        int page = c.path("pageSize").asInt(100);
        if (page < 1 || page > 100) {
            throw new ConnectorException.Invalid("pageSize must be between 1 and 100");
        }
        JsonNode inv = c.path("invoice");
        String method = inv.path("collectionMethod").asText("send_invoice");
        if (!method.equals("send_invoice") && !method.equals("charge_automatically")) {
            throw new ConnectorException.Invalid("invoice.collectionMethod must be send_invoice or charge_automatically");
        }
        int days = inv.path("daysUntilDue").asInt(30);
        if (days < 0 || days > 3650) {
            throw new ConnectorException.Invalid("invoice.daysUntilDue must be between 0 and 3650");
        }
        String key = c.path("link").path("metadataKey").asText("erp_payment_id");
        if (!key.matches("[A-Za-z0-9_\\-]{1,40}")) {
            throw new ConnectorException.Invalid("link.metadataKey must be 1 to 40 letters, digits, - or _ (Stripe's metadata keys)");
        }
        byte[] hook = null;
        String hookError = null;
        JsonNode w = c.path("webhook");
        if (c.has("webhook")) {
            String ref = w.path("secretRef").asText("");
            if (ref.isEmpty()) {
                throw new ConnectorException.Invalid("webhook.secretRef must name the webhook endpoint's signing secret (whsec_...)");
            }
            String v = e.secrets().get(ref);
            if (v == null || v.isBlank()) {
                // Polling does not need it: only a worker listening for webhooks does.
                hookError = "webhook.secretRef " + ref + " is not set: the invoice.paid webhook cannot be verified";
            } else {
                hook = v.trim().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        long tolerance = w.path("toleranceSeconds").asLong(300);
        if (tolerance < 1 || tolerance > 3600) {
            throw new ConnectorException.Invalid("webhook.toleranceSeconds must be between 1 and 3600");
        }
        return new StripeConfig(url, c.path("apiVersion").asText(""), page, method, days, inv.path("finalize").asBoolean(true), key, hook, tolerance, hookError);
    }

    /** Never prints the webhook's secret. */
    @Override
    public String toString() {
        return "StripeConfig[url=" + url + ", apiVersion=" + apiVersion + ", webhook=" + (webhookSecret != null) + "]";
    }
}
