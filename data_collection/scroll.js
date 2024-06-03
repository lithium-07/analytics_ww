// Add event listeners on window load
window.addEventListener("load", () => {
  window.addEventListener("scroll", debounce(scrollHandler, 200));
});

// Scroll thresholds
const SCROLL_THRESHOLDS = {
  25: false,
  50: false,
  75: false,
  100: false,
};

// Debounce function to limit the rate at which a function can fire
function debounce(func, wait) {
  let timeout;
  return function () {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, arguments), wait);
  };
}

// Scroll handler function
function scrollHandler() {
  const scrollTop = window.scrollY;
  const scrollHeight =
    document.documentElement.scrollHeight - window.innerHeight;
  const scrollPercent = (scrollTop / scrollHeight) * 100;

  // Check each threshold and fire events accordingly
  for (const [threshold, isFired] of Object.entries(SCROLL_THRESHOLDS)) {
    if (!isFired && scrollPercent >= threshold) {
      Shopify.analytics.publish("page_scroll", { percent: Number(threshold) });
      SCROLL_THRESHOLDS[threshold] = true;
    }
  }
}
