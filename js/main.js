/**
 * Main dashboard logic
 */

import { 
    loadJSON, 
    getStatusBadge, 
    getCheckIcon, 
    formatDate, 
    formatRelativeTime,
    getMostRecentTimestamp,
    debounce 
} from './utils.js';

// State
let allDatasets = [];
let registry = null;
let summary = null;
let currentSort = { column: 'id', direction: 'asc' };
let currentFilter = 'all';
let currentSearch = '';

/**
 * Initialize the dashboard
 */
async function init() {
    try {
        // Load data
        [registry, summary] = await Promise.all([
            loadJSON('../data/datasets-registry.json'),
            loadJSON('../data/all-datasets.json')
        ]);

        // Combine data
        allDatasets = summary.datasets.map(ds => ({
            ...ds,
            latestSnapshot: registry.latestSnapshots[ds.id]
        }));

        // Check for staleness
        checkStaleness();

        // Update summary stats
        updateSummaryStats();

        // Update footer
        document.getElementById('last-updated').textContent = formatDate(summary.lastUpdated);
        document.getElementById('schema-version').textContent = summary.schemaVersion;

        // Render table
        renderTable();

        // Setup event listeners
        setupEventListeners();

        // Show table, hide loading
        document.getElementById('loading').style.display = 'none';
        document.getElementById('table-container').style.display = 'block';

    } catch (error) {
        console.error('Error initializing dashboard:', error);
        document.getElementById('loading').style.display = 'none';
        document.getElementById('error').style.display = 'block';
        document.getElementById('error-message').textContent = error.message;
    }
}

/**
 * Check if summary is stale compared to registry
 */
function checkStaleness() {
    const summaryDate = new Date(summary.lastUpdated);
    const registryDate = new Date(registry.lastChecked);

    if (summaryDate < registryDate) {
        const warning = document.getElementById('staleness-warning');
        warning.style.display = 'block';
        document.getElementById('summary-age').textContent = formatRelativeTime(summary.lastUpdated);
    }
}

/**
 * Update summary statistics
 */
function updateSummaryStats() {
    const stats = {
        ok: 0,
        warning: 0,
        error: 0,
        'version-mismatch': 0,
        pending: 0
    };

    allDatasets.forEach(ds => {
        stats[ds.status] = (stats[ds.status] || 0) + 1;
    });

    document.getElementById('total-datasets').textContent = allDatasets.length;
    document.getElementById('count-ok').textContent = stats.ok;
    document.getElementById('count-warning').textContent = stats.warning;
    document.getElementById('count-error').textContent = stats.error;
    document.getElementById('count-version-mismatch').textContent = stats['version-mismatch'];
    document.getElementById('count-pending').textContent = stats.pending;
}

/**
 * Setup event listeners
 */
function setupEventListeners() {
    // Sort headers
    document.querySelectorAll('th.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const column = th.dataset.column;
            if (currentSort.column === column) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = column;
                currentSort.direction = 'asc';
            }
            renderTable();
        });
    });

    // Status filter
    document.getElementById('status-filter').addEventListener('change', (e) => {
        currentFilter = e.target.value;
        renderTable();
    });

    // Search box
    const searchBox = document.getElementById('search-box');
    searchBox.addEventListener('input', debounce((e) => {
        currentSearch = e.target.value.toLowerCase();
        renderTable();
    }, 300));
}

/**
 * Filter and sort datasets
 */
function getFilteredDatasets() {
    let filtered = [...allDatasets];

    // Apply status filter
    if (currentFilter !== 'all') {
        filtered = filtered.filter(ds => ds.status === currentFilter);
    }

    // Apply search
    if (currentSearch) {
        filtered = filtered.filter(ds => 
            ds.id.toLowerCase().includes(currentSearch)
        );
    }

    // Apply sort
    filtered.sort((a, b) => {
        let aVal, bVal;

        switch (currentSort.column) {
            case 'id':
                aVal = a.id;
                bVal = b.id;
                break;
            case 'latestSnapshot':
                aVal = a.latestSnapshot;
                bVal = b.latestSnapshot;
                break;
            case 'status':
                // Sort by severity
                const priority = { 'error': 0, 'version-mismatch': 1, 'warning': 2, 'pending': 3, 'ok': 4 };
                aVal = priority[a.status];
                bVal = priority[b.status];
                break;
            case 'lastChecked':
                aVal = getMostRecentTimestamp(a.lastChecked) || '';
                bVal = getMostRecentTimestamp(b.lastChecked) || '';
                break;
            default:
                return 0;
        }

        if (aVal < bVal) return currentSort.direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return currentSort.direction === 'asc' ? 1 : -1;
        return 0;
    });

    return filtered;
}

/**
 * Render the dataset table
 */
function renderTable() {
    const tbody = document.getElementById('dataset-table-body');
    const filtered = getFilteredDatasets();

    // Update sort indicators
    document.querySelectorAll('th.sortable').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
        if (th.dataset.column === currentSort.column) {
            th.classList.add(`sort-${currentSort.direction}`);
        }
    });

    // Show/hide no results message
    if (filtered.length === 0) {
        tbody.innerHTML = '';
        document.getElementById('no-results').style.display = 'block';
        return;
    } else {
        document.getElementById('no-results').style.display = 'none';
    }

    // Render rows
    tbody.innerHTML = filtered.map(ds => {
        const lastChecked = getMostRecentTimestamp(ds.lastChecked);
        
        return `
            <tr>
                <td>
                    <a href="dataset.html?id=${ds.id}" class="dataset-link">${ds.id}</a>
                </td>
                <td><code>${ds.latestSnapshot}</code></td>
                <td>${getStatusBadge(ds.status)}</td>
                <td>${getCheckIcon(ds.checks.github)}</td>
                <td>${getCheckIcon(ds.checks.s3Version)}</td>
                <td>${getCheckIcon(ds.checks.s3Files)}</td>
                <td>${formatRelativeTime(lastChecked)}</td>
            </tr>
        `;
    }).join('');
}

// Initialize on load
init();
