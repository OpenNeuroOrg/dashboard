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
        'ok': { text: '✓ OK', class: 'ok' },
        'warning': { text: '⚠ Warning', class: 'warning' },
        'error': { text: '✗ Error', class: 'error' },
        'version-mismatch': { text: '≠ Version Mismatch', class: 'version-mismatch' },
        'pending': { text: '⏳ Pending', class: 'pending' }
    };
    
    const badge = badges[status] || { text: status, class: 'pending' };
    return `<span class="status-badge ${badge.class}">${badge.text}</span>`;
}

/**
 * Get check icon HTML
 */
export function getCheckIcon(status) {
    const icons = {
        'ok': { icon: '✓', class: 'ok' },
        'warning': { icon: '⚠', class: 'warning' },
        'error': { icon: '✗', class: 'error' },
        'version-mismatch': { icon: '≠', class: 'version-mismatch' },
        'pending': { icon: '⏳', class: 'pending' }
    };
    
    const icon = icons[status] || { icon: '?', class: 'pending' };
    return `<span class="check-icon ${icon.class}" title="${status}">${icon.icon}</span>`;
}

/**
 * Format ISO date string as YYYY-MM-DD @ HH:MM TZ
 */
export function formatDate(isoString) {
    if (!isoString) return '-';
    
    const date = new Date(isoString);
    
    // Get components
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    
    // Get timezone abbreviation
    const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const tzShort = date.toLocaleTimeString('en-US', { 
        timeZoneName: 'short' 
    }).split(' ').pop();
    
    return `${year}-${month}-${day} @ ${hours}:${minutes} ${tzShort}`;
}

/**
 * Format relative time (e.g., "2 hours ago")
 */
export function formatRelativeTime(isoString) {
    if (!isoString) return '-';
    
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins} minute${diffMins > 1 ? 's' : ''} ago`;
    if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`;
    if (diffDays < 7) return `${diffDays} day${diffDays > 1 ? 's' : ''} ago`;
    
    return formatDate(isoString);
}

/**
 * Get the most recent timestamp from an object of timestamps
 */
export function getMostRecentTimestamp(timestamps) {
    if (!timestamps) return null;
    
    const times = Object.values(timestamps)
        .filter(t => t)
        .map(t => new Date(t));
    
    if (times.length === 0) return null;
    
    return new Date(Math.max(...times)).toISOString();
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
