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
        const registry = await loadJSON('data/datasets-registry.json');
        latestSnapshot = registry.latestSnapshots[datasetId];

        if (!latestSnapshot) {
            showError(`Dataset ${datasetId} not found in registry`);
            return;
        }

        // Load dataset-specific files
        const basePath = `data/datasets/${datasetId}`;

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
        await renderS3FilesCheck();

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
                    `data/datasets/${datasetId}/snapshots/${tag}/metadata.json`
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
    
    if (!s3Version) {
        icon.textContent = '⏳';
        icon.className = 'check-icon-large pending';
        document.getElementById('s3-version-summary').textContent = 'Check not yet run';
        document.getElementById('s3-version-details').style.display = 'none';
        return;
    }

    // Case 3: Blocked (403)
    if (!s3Version.accessible) {
        icon.textContent = '🔒';
        icon.className = 'check-icon-large error';
        
        const summary = `Access to S3 files is forbidden (HTTP 403). The dataset is marked as ` +
                       `public in OpenNeuro but the S3 bucket ACL needs to be updated. ` +
                       `No file validation can be performed until access is restored.`;
        
        document.getElementById('s3-version-summary').textContent = summary;
        
        // Show minimal details
        document.getElementById('s3-doi').textContent = 'N/A (access denied)';
        document.getElementById('s3-extracted-version').textContent = 'N/A';
        document.getElementById('s3-expected-version').textContent = latestSnapshot;
        document.getElementById('s3-version-last-checked').textContent = formatDate(s3Version.lastChecked);
        
        return;
    }

    // Cases 1, 2, 4: Accessible
    icon.textContent = status === 'ok' ? '✓' : status === 'version-mismatch' ? '≠' : '⚠';
    icon.className = `check-icon-large ${status}`;

    // Build summary message
    let summary = '';
    const versionSource = s3Version.versionSource || 'unknown';
    const extractedVersion = s3Version.extractedVersion || 'unknown';
    
    if (versionSource === 'doi') {
        // Case 1: DOI with version
        if (status === 'ok') {
            summary = `✓ S3 DOI indicates version ${extractedVersion}, which matches the latest snapshot.`;
        } else {
            summary = `≠ S3 DOI indicates version ${extractedVersion}, but latest snapshot is ${latestSnapshot}.`;
        }
        
        if (s3Version.doiIdMismatch) {
            summary += ` ⚠ DOI contains wrong dataset ID (${s3Version.doiDatasetId}).`;
        }
    } else if (versionSource === 'assumed_latest') {
        // Case 2 or 4: Assumed latest
        summary = `⚠ Could not extract version from DOI`;
        
        if (s3Version.datasetDescriptionMissing) {
            // Case 4: 404
            summary += ` (dataset_description.json not found - HTTP 404)`;
        } else if (!s3Version.datasetDescriptionDOI) {
            // Case 2: Missing DOI field
            summary += ` (no DatasetDOI field)`;
        } else {
            // Case 2: Custom DOI
            summary += ` (custom DOI: ${s3Version.datasetDescriptionDOI})`;
        }
        
        summary += `. Assuming latest snapshot (${latestSnapshot}) for file comparison.`;
    }
    
    document.getElementById('s3-version-summary').textContent = summary;

    // Details
    document.getElementById('s3-doi').textContent = s3Version.datasetDescriptionDOI || 'N/A';
    document.getElementById('s3-extracted-version').textContent = extractedVersion;
    document.getElementById('s3-expected-version').textContent = latestSnapshot;
    document.getElementById('s3-version-last-checked').textContent = formatDate(s3Version.lastChecked);
}

/**
 * Render S3 files check section
 */
function renderS3FilesCheck() {
    const status = getS3FilesStatus();
    const icon = document.getElementById('s3-files-icon');

    if (s3Version && !s3Version.accessible) {
        icon.textContent = '🔒';
        icon.className = 'check-icon-large error';
        document.getElementById('s3-files-summary').textContent =
            'File check blocked - S3 access denied (see S3 Version Status above)';
        document.getElementById('s3-diff-container').style.display = 'none';
        return;
    }

    icon.textContent = status === 'ok' ? '✓' : status === 'error' ? '✗' : status === 'version-mismatch' ? '≠' : '⏳';
    icon.className = `check-icon-large ${status}`;

    if (!s3Version) {
        document.getElementById('s3-files-summary').textContent = 'S3 version check not yet run';
        return;
    }

    if (!s3Diff) {
        document.getElementById('s3-files-summary').textContent = 'File comparison not yet run';
        return;
    }

    if (s3Diff.exportMissing) {
        const summary = `No files found on S3 (0/${s3Diff.totalGitFiles}). ` +
                       `The dataset export appears to be completely missing and needs to be regenerated.`;
        document.getElementById('s3-files-summary').textContent = summary;
        document.getElementById('s3-diff-container').style.display = 'none';
        return;
    }

    const addedCount = s3Diff.added.length;
    const removedCount = s3Diff.removed.length;

    let summary = '';
    if (status === 'ok') {
        summary = `✓ All ${s3Diff.totalGitFiles} files match between Git and S3.`;
    } else {
        summary = `✗ File mismatch: ${addedCount} file${addedCount !== 1 ? 's' : ''} missing from S3, ` +
                 `${removedCount} extra file${removedCount !== 1 ? 's' : ''} in S3.`;
    }
    document.getElementById('s3-files-summary').textContent = summary;

    document.getElementById('s3-diff-container').style.display = 'block';

    document.getElementById('diff-total-git').textContent = s3Diff.totalGitFiles;
    document.getElementById('diff-total-s3').textContent = s3Diff.totalS3Files;
    document.getElementById('diff-added').textContent = addedCount;
    document.getElementById('diff-removed').textContent = removedCount;

    if (addedCount > 0 || removedCount > 0) {
        renderDiffViewer(s3Diff);
    }

    document.getElementById('diff-snapshot-tag').textContent = s3Diff.snapshotTag;
    document.getElementById('diff-s3-version').textContent = s3Diff.s3Version;
    document.getElementById('s3-files-last-checked').textContent = formatDate(s3Diff.checkedAt);
}

/**
 * Get S3 files check status
 */
function getS3FilesStatus() {
    if (!s3Version) return 'pending';
    if (s3Version.extractedVersion !== latestSnapshot) return 'version-mismatch';
    if (!s3Diff) return 'pending';
    return s3Diff.status || 'pending';
}

/**
 * Render unified diff viewer from added/removed/context lists
 */
function renderDiffViewer(diff) {
    const viewer = document.getElementById('diff-viewer');

    const unified = [
        ...diff.added.map(f => ({ file: f, type: 'added' })),
        ...diff.removed.map(f => ({ file: f, type: 'removed' })),
        ...diff.context.map(f => ({ file: f, type: 'context' })),
    ].sort((a, b) => a.file.localeCompare(b.file));

    const CONTEXT_SIZE = 3;
    const lines = [];
    let contextBuffer = [];

    function flushContext() {
        if (contextBuffer.length === 0) return;

        if (contextBuffer.length > CONTEXT_SIZE * 2) {
            lines.push(...contextBuffer.slice(0, CONTEXT_SIZE));
            const hidden = contextBuffer.slice(CONTEXT_SIZE, -CONTEXT_SIZE);
            if (hidden.length > 0) {
                lines.push({ type: 'collapse', count: hidden.length, files: hidden });
            }
            lines.push(...contextBuffer.slice(-CONTEXT_SIZE));
        } else {
            lines.push(...contextBuffer);
        }
        contextBuffer = [];
    }

    for (const entry of unified) {
        if (entry.type === 'context') {
            contextBuffer.push(entry);
        } else {
            flushContext();
            lines.push(entry);
        }
    }
    flushContext();

    let html = '';
    let collapseId = 0;

    for (const line of lines) {
        if (line.type === 'collapse') {
            const id = `collapse-${collapseId++}`;
            html += `
                <button class="diff-collapse" data-target="${id}">
                    ${line.count} unchanged file${line.count !== 1 ? 's' : ''}
                </button>
                <div class="diff-section" id="${id}">
                    ${line.files.map(f =>
                        `<div class="diff-line context">  ${escapeHtml(f.file)}</div>`
                    ).join('')}
                </div>
            `;
        } else {
            const prefix = line.type === 'added' ? '+ ' : line.type === 'removed' ? '- ' : '  ';
            html += `<div class="diff-line ${line.type}">${prefix}${escapeHtml(line.file)}</div>`;
        }
    }

    viewer.innerHTML = html;

    viewer.querySelectorAll('.diff-collapse').forEach(button => {
        button.addEventListener('click', () => {
            const targetId = button.dataset.target;
            const section = document.getElementById(targetId);
            section.classList.toggle('expanded');
            button.classList.toggle('expanded');
        });
    });
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
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
                `data/datasets/${datasetId}/snapshots/${latestSnapshot}/files.json`
            );

            document.getElementById('files-count').textContent = files.count;
            document.getElementById('files-list').innerHTML = files.files
                .map(file => `<div>${escapeHtml(file)}</div>`)
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
