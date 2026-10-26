package dev.turgon.connector.shopify;

import com.fasterxml.jackson.databind.JsonNode;
import dev.turgon.connector.sdk.ConnectorException;
import dev.turgon.connector.sdk.EndpointConfig;
import java.net.URI;
import java.util.regex.Pattern;

/**
 * An endpoint's configuration (the connection's {@code config}):
 *
 * <pre>
 * shop: turgon-demo.myshopify.com   # the store's myshopify.com domain
 * apiVersion: 2026-07               # the Admin API version, supported for a year
 * url: https://turgon-demo.myshopify.com   # default: https:// and the shop; tests point it at a fake
 * pageSize: 50                      # orders per poll, at most 250
 * </pre>
 *
 * The secret is the app's Admin API access token ({@code shpat_...}), with
 * the scopes read_orders, write_orders and read_customers.
 */
public record ShopifyConfig(String shop, String apiVersion, URI url, int pageSize) {
    private static final Pattern SHOP = Pattern.compile("[a-z0-9][a-z0-9-]*\\.myshopify\\.com");
    private static final Pattern VERSION = Pattern.compile("\\d{4}-(01|04|07|10)|unstable");

    public static ShopifyConfig from(EndpointConfig e) throws ConnectorException.Invalid {
        JsonNode c = e.config();
        String shop = c.path("shop").asText("").trim().toLowerCase(java.util.Locale.ROOT);
        if (!SHOP.matcher(shop).matches()) {
            throw new ConnectorException.Invalid("shop must be the store's myshopify.com domain, such as acme.myshopify.com");
        }
        String version = c.path("apiVersion").asText("").trim();
        if (!VERSION.matcher(version).matches()) {
            throw new ConnectorException.Invalid("apiVersion must be a Shopify API version, such as 2026-07 (versions are released quarterly)");
        }
        String u = c.path("url").asText("https://" + shop).replaceAll("/+$", "");
        URI url;
        try {
            url = URI.create(u);
            if (url.getScheme() == null || url.getHost() == null) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException x) {
            throw new ConnectorException.Invalid("url: not an absolute URL");
        }
        int page = c.path("pageSize").asInt(50);
        if (page < 1 || page > 250) {
            throw new ConnectorException.Invalid("pageSize must be between 1 and 250");
        }
        return new ShopifyConfig(shop, version, url, page);
    }

    public URI endpoint() {
        return URI.create(url + "/admin/api/" + apiVersion + "/graphql.json");
    }
}
