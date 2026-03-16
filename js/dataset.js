/**
 * Dataset detail view logic
 */

import {
    loadJSON,
    getStatusBadge,
    getCheckIcon,
    formatDate,
    formatRelativeTime
} from './utils.js';

// State
let datasetId = null;
let latestSnapshot = null;
let snapshots = null;
let github = null;
let s3Version = null;
let s3Diff = null;

/**
 * Initialize the dataset detail view
 */
async function init() {
    // Get dataset ID from URL
    const params = new URLSearchParams(window.location.search);
    datasetId = params.get('id');

    if (!datasetId) {
        showError('No dataset ID provided in URL');
        return;
    }

    // Update breadcrumb
    document.getElementById('breadcrumb-dataset').textContent = datasetId;
    document.getElementById('dataset-id').textContent = datasetId;

    try {
        // Load registry to get latest snapshot
        const registry = await loadJSON('../data/datasets-registry.json');
        latestSnapshot = registry.latestSnapshots[datasetId];

        if (!latestSnapshot) {
            showError(`Dataset ${datasetId} not found in registry`);
            return;
        }

        // Load dataset-specific files
        const basePath = `../data/datasets/${datasetId}`;
        
        [snapshots, github, s3Version, s3Diff] = await Promise.all([
            loadJSON(`${basePath}/snapshots.json`),
            loadJSON(`${basePath}/github.json`).catch(() => null),
            loadJSON(`${basePath}/s3-version.json`).catch(() => null),
            loadJSON(`${basePath}/s3-diff.json`).catch(() => null)
        ]);

        // Render all sections
        renderHeader();
        renderSnapshots();
        renderGitHubCheck();
        renderS3VersionCheck();
        renderS3FilesCheck();

        // Setup file list loader
        setupFileListLoader();

        // Show content, hide loading
        document.getElementById('loading').style.display = 'none';
        document.getElementById('dataset-details').style.display = 'block';

    } catch (error) {
        console.error('Error loading dataset:', error);
        showError(error.message);
    }
}

/**
 * Show error state
 */
function showError(message) {
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error').style.display = 'block';
    document.getElementById('error-message').textContent = message;
}

/**
 * Render dataset header
 */
function renderHeader() {
    document.getElementById('latest-snapshot').textContent = latestSnapshot;
    
    // Determine overall status
    const status = determineOverallStatus();
    document.getElementById('overall-status').innerHTML = getStatusBadge(status);
}

/**
 * Determine overall status from check results
 */
function determineOverallStatus() {
    const statuses = [];
    
    if (github) {
        statuses.push(getGitHubStatus());
    }
    if (s3Version) {
        statuses.push(getS3VersionStatus());
    }
    if (s3Version && s3Version.extractedVersion === latestSnapshot) {
        statuses.push(getS3FilesStatus());
    }
    
    // Worst status wins
    const priority = { 'error': 0, 'version-mismatch': 1, 'warning': 2, 'pending': 3, 'ok': 4 };
    return statuses.length > 0 
        ? statuses.reduce((worst, current) => priority[current] < priority[worst] ? current : worst)
        : 'pending';
}

/**
 * Render snapshots section
 */
function renderSnapshots() {
    const count = snapshots.tags.length;
    document.getElementById('snapshot-count').textContent = 
        `${count} snapshot${count !== 1 ? 's' : ''}`;

    const container = document.getElementById('snapshots-list');
    
    // Load metadata for each snapshot and render
    Promise.all(
        snapshots.tags.map(async tag => {
            try {
                const metadata = await loadJSON(
                    `../data/datasets/${datasetId}/snapshots/${tag}/metadata.json`
                );
                return { tag, ...metadata };
            } catch {
                return { tag, hexsha: 'unknown', created: null };
            }
        })
    ).then(snapshotData => {
        // Reverse to show newest first
        container.innerHTML = snapshotData.reverse().map(snap => `
            <div class="snapshot-item ${snap.tag === latestSnapshot ? 'latest' : ''}">
                <div class="snapshot-tag">
                    ${snap.tag}
                    ${snap.tag === latestSnapshot ? '<small>(latest)</small>' : ''}
                </div>
                <div class="snapshot-sha" title="${snap.hexsha}">${snap.hexsha.substring(0, 12)}...</div>
                <div class="snapshot-date">${formatDate(snap.created)}</div>
            </div>
        `).join('');
    });
}

/**
 * Get GitHub check status
 */
function getGitHubStatus() {
    if (!github) return 'pending';
    
    if (!(latestSnapshot in github.tags)) {
        return 'error';
    }
    
    if (github.branches[github.head] !== github.tags[latestSnapshot]) {
        return 'warning';
    }
    
    return 'ok';
}

/**
 * Render GitHub check section
 */
function renderGitHubCheck() {
    const status = getGitHubStatus();
    const icon = document.getElementById('github-icon');
    icon.textContent = status === 'ok' ? '✓' : status === 'warning' ? '⚠' : status === 'error' ? '✗' : '⏳';
    icon.className = `check-icon-large ${status}`;

    if (!github) {
        document.getElementById('github-summary').textContent = 'Check not yet run';
        document.getElementById('github-details').style.display = 'none';
        return;
    }

    // Summary message
    let summary = '';
    if (status === 'ok') {
        summary = `✓ Latest tag (${latestSnapshot}) exists on GitHub and HEAD points to it.`;
    } else if (status === 'error') {
        summary = `✗ Latest tag (${latestSnapshot}) is missing from GitHub mirror.`;
    } else if (status === 'warning') {
        summary = `⚠ Latest tag exists but HEAD (${github.head}) points to a different commit.`;
    }
    document.getElementById('github-summary').textContent = summary;

    // Details
    document.getElementById('github-head').textContent = 
        `${github.head} → ${github.branches[github.head] || 'unknown'}`;
    
    document.getElementById('github-branches').innerHTML = Object.entries(github.branches)
        .map(([name, sha]) => `<div><strong>${name}:</strong> ${sha}</div>`)
        .join('');
    
    document.getElementById('github-tags').innerHTML = Object.entries(github.tags)
        .map(([name, sha]) => {
            const isLatest = name === latestSnapshot;
            const style = isLatest ? 'color: var(--primary); font-weight: bold;' : '';
            return `<div style="${style}"><strong>${name}:</strong> ${sha}</div>`;
        })
        .join('');
    
    document.getElementById('github-last-checked').textContent = formatDate(github.lastChecked);
}

/**
 * Get S3 version check status
 */
function getS3VersionStatus() {
    if (!s3Version) return 'pending';
    return s3Version.extractedVersion === latestSnapshot ? 'ok' : 'version-mismatch';
}

/**
 * Render S3 version check section
 */
function renderS3VersionCheck() {
    const status = getS3VersionStatus();
    const icon = document.getElementById('s3-version-icon');
    icon.textContent = status === 'ok' ? '✓' : status === 'version-mismatch' ? '≠' : '⏳';
    icon.className = `check-icon-large ${status}`;

    if (!s3Version) {
        document.getElementById('s3-version-summary').textContent = 'Check not yet run';
        document.getElementById('s3-version-details').style.display = 'none';
        return;
    }

    // Summary message
    let summary = '';
    if (status === 'ok') {
        summary = `✓ S3 reports version ${s3Version.extractedVersion}, which matches the latest snapshot.`;
    } else {
        summary = `≠ S3 reports version ${s3Version.extractedVersion}, but latest snapshot is ${latestSnapshot}.`;
    }
    document.getElementById('s3-version-summary').textContent = summary;

    // Details
    document.getElementById('s3-doi').textContent = s3Version.datasetDescriptionDOI;
    document.getElementById('s3-extracted-version').textContent = s3Version.extractedVersion;
    document.getElementById('s3-expected-version').textContent = latestSnapshot;
    document.getElementById('s3-version-last-checked').textContent = formatDate(s3Version.lastChecked);
}

/**
 * Get S3 files check status
 */
function getS3FilesStatus() {
    if (!s3Version) return 'pending';
    if (s3Version.extractedVersion !== latestSnapshot) return 'version-mismatch';
    if (!s3Diff) return 'pending';
    
    if (s3Diff.summary.inGitOnly > 0 || s3Diff.summary.inS3Only > 0) {
        return 'error';
    }
    
    return 'ok';
}

/**
 * Render S3 files check section
 */
function renderS3FilesCheck() {
    const status = getS3FilesStatus();
    const icon = document.getElementById('s3-files-icon');
    icon.textContent = status === 'ok' ? '✓' : status === 'error' ? '✗' : status === 'version-mismatch' ? '≠' : '⏳';
    icon.className = `check-icon-large ${status}`;

    if (!s3Version) {
        document.getElementById('s3-files-summary').textContent = 'S3 version check not yet run';
        return;
    }

    if (s3Version.extractedVersion !== latestSnapshot) {
        document.getElementById('s3-files-summary').textContent = 
            `Version mismatch - S3 diff not computed (S3 is at ${s3Version.extractedVersion}, not ${latestSnapshot})`;
        return;
    }

    if (!s3Diff) {
        document.getElementById('s3-files-summary').textContent = 'File comparison not yet run';
        return;
    }

    // Summary message
    let summary = '';
    if (status === 'ok') {
        summary = `✓ All ${s3Diff.summary.inBoth} files match between Git and S3.`;
    } else {
        const missing = s3Diff.summary.inGitOnly;
        const extra = s3Diff.summary.inS3Only;
        summary = `✗ File mismatch: ${missing} file${missing !== 1 ? 's' : ''} missing from S3, ${extra} extra file${extra !== 1 ? 's' : ''} in S3.`;
    }
    document.getElementById('s3-files-summary').textContent = summary;

    // Show diff details
    document.getElementById('s3-diff-container').style.display = 'block';
    
    // Populate diff summary
    document.getElementById('diff-total-git').textContent = s3Diff.summary.totalInGit;
    document.getElementById('diff-total-s3').textContent = s3Diff.summary.totalInS3;
    document.getElementById('diff-in-both').textContent = s3Diff.summary.inBoth;
    document.getElementById('diff-git-only').textContent = s3Diff.summary.inGitOnly;
    document.getElementById('diff-s3-only').textContent = s3Diff.summary.inS3Only;

    // Show file lists if there are differences
    if (s3Diff.inGitOnly.length > 0) {
        const section = document.getElementById('files-git-only');
        section.style.display = 'block';
        section.querySelector('.file-list').innerHTML = s3Diff.inGitOnly
            .map(file => `<div>${file}</div>`)
            .join('');
    }

    if (s3Diff.inS3Only.length > 0) {
        const section = document.getElementById('files-s3-only');
        section.style.display = 'block';
        section.querySelector('.file-list').innerHTML = s3Diff.inS3Only
            .map(file => `<div>${file}</div>`)
            .join('');
    }

    // Other details
    document.getElementById('diff-git-sha').textContent = s3Diff.gitHexsha;
    document.getElementById('diff-s3-version').textContent = s3Diff.s3Version;
    document.getElementById('s3-files-last-checked').textContent = formatDate(s3Diff.lastChecked);
}

/**
 * Setup file list loader
 */
function setupFileListLoader() {
    const btn = document.getElementById('load-files-btn');
    const container = document.getElementById('files-container');
    
    document.getElementById('files-snapshot-tag').textContent = latestSnapshot;
    
    btn.addEventListener('click', async () => {
        btn.setAttribute('aria-busy', 'true');
        btn.textContent = 'Loading...';
        
        try {
            const files = await loadJSON(
                `../data/datasets/${datasetId}/snapshots/${latestSnapshot}/files.json`
            );
            
            document.getElementById('files-count').textContent = files.count;
            document.getElementById('files-list').innerHTML = files.files
                .map(file => `<div>${file}</div>`)
                .join('');
            
            container.style.display = 'block';
            btn.style.display = 'none';
            
        } catch (error) {
            alert(`Failed to load file list: ${error.message}`);
        } finally {
            btn.removeAttribute('aria-busy');
            btn.textContent = 'Load File List';
        }
    });
}

// Initialize on load
init();
