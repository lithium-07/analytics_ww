import { register } from "@shopify/web-pixels-extension";

// Backend URL for event ingestion
const BACKEND_URL = process.env.BACKEND_URL || "<your_backend_url>/ingest-gcp";

/**
 * Check if the device is mobile based on the user agent string.
 * @param {string} userAgent - The user agent string to check.
 * @returns {boolean} - True if the device is mobile, otherwise false.
 */
function isMobile(userAgent) {
  const mobileRegex =
    /Mobile|Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i;
  return mobileRegex.test(userAgent);
}

/**
 * Get or set the session ID in the browser cookies.
 * @param {object} browser - The browser object to interact with cookies.
 * @returns {Promise<string>} - The session ID.
 */
async function getSessionId(browser) {
  let sessionId = await browser.cookie.get("session_id");
  if (!sessionId) {
    sessionId = `sess-${Math.random().toString(36).substring(2)}`;
    await browser.cookie.set("session_id", sessionId);
  }
  return sessionId;
}

/**
 * Send an event payload to the backend server.
 * @param {object} payload - The event payload to send.
 */
async function sendEvent(payload) {
  try {
    const response = await fetch(BACKEND_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
      keepalive: true,
    });

    if (!response.ok) {
      throw new Error("Failed to send event to server.");
    }

    console.log("Event sent successfully:", payload);
  } catch (error) {
    console.error("Error sending event:", error);
  }
}

/**
 * Transform and send an event to the backend server.
 * @param {object} event - The event object to transform and send.
 * @param {string} sessionId - The session ID.
 * @param {string} platform - The platform type (mobile or web).
 * @param {object} additionalData - Additional data to include in the payload.
 */
async function handleEvent(event, sessionId, platform, additionalData = {}) {
  const transformedPayload = {
    platform,
    event_time: event.timestamp,
    event_name: event.name,
    event_id: event.id,
    user_id: event.clientId,
    session_id: sessionId,
    page_url: event.context.document.location.host,
    user_agent: event.context.navigator.userAgent,
    language: event.context.navigator.language,
    ...additionalData,
  };

  await sendEvent(transformedPayload);
}

register(async ({ analytics, browser }) => {
  analytics.subscribe("page_scroll", async (event) => {
    const sessionId = await getSessionId(browser);
    const platform = isMobile(event.context.navigator.userAgent)
      ? "mobile"
      : "web";
    const additionalData = {
      page_title: event.context.document.title,
      percent_scroll: event.customData.percent,
    };

    await handleEvent(event, sessionId, platform, additionalData);
  });

  analytics.subscribe("page_viewed", async (event) => {
    const sessionId = await getSessionId(browser);
    const platform = isMobile(event.context.navigator.userAgent)
      ? "mobile"
      : "web";

    await handleEvent(event, sessionId, platform);
  });

  analytics.subscribe("product_added_to_cart", async (event) => {
    const sessionId = await getSessionId(browser);
    const platform = isMobile(event.context.navigator.userAgent)
      ? "mobile"
      : "web";
    const additionalData = {
      product_id: event.data.cartLine.merchandise.product.id,
      product_price: event.data.cartLine.merchandise.price.amount,
      currency: event.data.cartLine.merchandise.price.currencyCode,
    };

    await handleEvent(event, sessionId, platform, additionalData);
  });

  analytics.subscribe("checkout_completed", async (event) => {
    const sessionId = await getSessionId(browser);
    const platform = isMobile(event.context.navigator.userAgent)
      ? "mobile"
      : "web";
    const additionalData = {
      order_id: event.data.checkout.order.id,
      order_value: event.data.checkout.totalPrice.amount,
      city: event.data.checkout.shippingAddress.city,
      country: event.data.checkout.shippingAddress.country,
      currency: event.data.checkout.totalPrice.currencyCode,
      user_email: event.data.checkout.email,
    };

    await handleEvent(event, sessionId, platform, additionalData);
  });
});
