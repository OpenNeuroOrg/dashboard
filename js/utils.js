/**
 * Utility functions for the OpenNeuro Dashboard
 */

/**
 * Load JSON from a path
 */
export async function loadJSON(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.statusText}`);
  }
  return response.json();
}

/**
 * Get status badge HTML
 */
export function getStatusBadge(status) {
  const badges = {
    ok: { text: "✓ OK", class: "ok" },
    warning: { text: "⚠ Warning", class: "warning" },
    error: { text: "✗ Error", class: "error" },
    "version-mismatch": {
      text: "≠ Version Mismatch",
      class: "version-mismatch",
    },
    pending: { text: "⏳ Pending", class: "pending" },
  };

  const badge = badges[status] || { text: status, class: "pending" };
  return `<span class="status-badge ${badge.class}">${badge.text}</span>`;
}

/**
 * Get check icon HTML
 * @param {string} status - The check status
 * @param {boolean} blocked - Whether the check is blocked (e.g., 403)
 */
export function getCheckIcon(status, blocked = false) {
  const icons = {
    ok: { icon: "✓", class: "ok" },
    warning: { icon: "⚠", class: "warning" },
    error: { icon: "✗", class: "error" },
    "version-mismatch": { icon: "≠", class: "version-mismatch" },
    pending: { icon: "⏳", class: "pending" },
  };

  const icon = icons[status] || { icon: "?", class: "pending" };

  // Use lock icon if blocked
  const displayIcon = blocked ? "🔒" : icon.icon;
  const title = blocked ? `Blocked (403) - ${status}` : status;

  return `<span class="check-icon ${icon.class}" title="${title}">${displayIcon}</span>`;
}

/**
 * Format ISO date string as YYYY-MM-DD @ HH:MM TZ
 */
export function formatDate(isoString) {
  if (!isoString) return "-";

  const date = new Date(isoString);

  // Get components
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");

  // Get timezone abbreviation
  const tzShort = date
    .toLocaleTimeString("en-US", {
      timeZoneName: "short",
    })
    .split(" ")
    .pop();

  return `${year}-${month}-${day} @ ${hours}:${minutes} ${tzShort}`;
}

/**
 * Format relative time (e.g., "2 hours ago")
 */
export function formatRelativeTime(isoString) {
  if (!isoString) return "-";

  const date = new Date(isoString);
  const now = new Date();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins} minute${diffMins > 1 ? "s" : ""} ago`;
  if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? "s" : ""} ago`;
  if (diffDays < 7) return `${diffDays} day${diffDays > 1 ? "s" : ""} ago`;

  return formatDate(isoString);
}

/**
 * Get the most recent timestamp from an object of timestamps
 */
export function getMostRecentTimestamp(timestamps) {
  if (!timestamps) return null;

  const times = Object.values(timestamps)
    .filter((t) => t)
    .map((t) => new Date(t));

  if (times.length === 0) return null;

  return new Date(Math.max(...times)).toISOString();
}

/**
 * Get composite status dots HTML (3 dots for GitHub, S3 Version, S3 Files)
 */
export function getStatusDots(checks, s3Blocked = false) {
  const dotFor = (status, label, blocked = false) => {
    const cls = blocked ? "error" : status;
    const title = blocked ? `${label}: Blocked (403)` : `${label}: ${status}`;
    const icon = blocked ? "\uD83D\uDD12" : "";
    return `<span class="status-dot ${cls}" title="${title}">${icon}</span>`;
  };
  return (
    `<span class="status-dots">` +
    dotFor(checks.github, "GitHub") +
    dotFor(checks.s3Version, "S3 Version", s3Blocked) +
    dotFor(checks.s3Files, "S3 Files", s3Blocked) +
    `</span>`
  );
}

/**
 * Format a number compactly (e.g., 1234 -> "1.2k")
 */
export function formatCompactNumber(n) {
  if (n === null || n === undefined) return "\u2014";
  if (n < 1000) return String(n);
  if (n < 10000) return (n / 1000).toFixed(1) + "k";
  if (n < 1000000) return Math.round(n / 1000) + "k";
  return (n / 1000000).toFixed(1) + "M";
}

/**
 * Format diff counts as compact +added/-removed HTML
 */
export function formatDiffCounts(added, removed) {
  if (added === null && removed === null) return "";
  if (added === undefined && removed === undefined) return "";
  const a = added || 0;
  const r = removed || 0;
  if (a === 0 && r === 0) return "\u2014";
  const fullCount = a + r;
  return (
    `<span class="diff-compact" aria-label="${fullCount.toLocaleString()} file differences">` +
    `<span class="diff-added">+${formatCompactNumber(a)}</span>` +
    `<span class="diff-sep">/</span>` +
    `<span class="diff-removed">\u2212${formatCompactNumber(r)}</span>` +
    `</span>`
  );
}

/**
 * Debounce function for search input
 */
export function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}
