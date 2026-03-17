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

    // Handle error cases
    if (s3Version.error) {
        const errorMessages = {
            'no_doi_field': 'No DatasetDOI field found in dataset_description.json',
            'custom_doi': 'Dataset uses a custom DOI (not versioned)',
            'doi_id_mismatch': `DOI contains wrong dataset ID: ${s3Version.doi_dataset_id || 'unknown'}`,
            'not_found': 'dataset_description.json not found on S3 (404)',
            'access_denied': 'Access denied - dataset may be tagged as private on S3 (403)',
            'http_error': `HTTP error ${s3Version.http_status || 'unknown'}`,
            'request_error': 'Network request failed',
            'invalid_json': 'Invalid JSON in dataset_description.json',
            'unexpected_error': s3Version.error_message || 'Unexpected error occurred'
        };

        const message = errorMessages[s3Version.error] || `Error: ${s3Version.error}`;

        document.getElementById('s3-version-summary').textContent = `✗ ${message}`;

        // Show details with error information
        document.getElementById('s3-doi').textContent = s3Version.datasetDescriptionDOI || 'N/A';
        document.getElementById('s3-extracted-version').textContent = s3Version.extractedVersion || 'N/A';
        document.getElementById('s3-expected-version').textContent = latestSnapshot;
        document.getElementById('s3-version-last-checked').textContent = formatDate(s3Version.lastChecked);

        // Update icon to show error
        const icon = document.getElementById('s3-version-icon');
        icon.textContent = '✗';
        icon.className = 'check-icon-large error';

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
async function renderS3FilesCheck() {
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

    // Other details
    document.getElementById('diff-git-sha').textContent = s3Diff.gitHexsha;
    document.getElementById('diff-s3-version').textContent = s3Diff.s3Version;
    document.getElementById('s3-files-last-checked').textContent = formatDate(s3Diff.lastChecked);

    // Load git files and render diff
    try {
        const gitFiles = await loadJSON(
            `../data/datasets/${datasetId}/snapshots/${latestSnapshot}/files.json`
        );
        renderDiffViewer(gitFiles.files, s3Diff);
    } catch (error) {
        console.error('Failed to load git files for diff:', error);
        document.getElementById('diff-viewer').innerHTML =
            '<div style="padding: 1rem; color: var(--status-error);">Failed to load git files for diff view</div>';
    }
}

/**
 * Render unified diff viewer
 */
function renderDiffViewer(gitFiles, diff) {
    const viewer = document.getElementById('diff-viewer');

    // Create sets for fast lookup
    const inGitOnly = new Set(diff.inGitOnly);
    const inS3Only = new Set(diff.inS3Only);

    // Combine all files (git files + S3-only files) and sort
    const allFiles = [...gitFiles, ...diff.inS3Only].sort();

    // Build diff with context grouping
    const lines = [];
    const CONTEXT_SIZE = 3; // Show 3 lines of context around changes

    let lastChangeIndex = -1000; // Track last line with changes
    let unchangedBuffer = [];

    allFiles.forEach((file, index) => {
        const isRemoved = inGitOnly.has(file);
        const isAdded = inS3Only.has(file);
        const isChanged = isRemoved || isAdded;

        if (isChanged) {
            // Flush unchanged buffer if we have a new change
            if (unchangedBuffer.length > 0) {
                const distanceFromLastChange = index - lastChangeIndex;

                if (distanceFromLastChange > CONTEXT_SIZE * 2 + 1) {
                    // Large gap - create collapsible section
                    const contextBefore = unchangedBuffer.slice(0, CONTEXT_SIZE);
                    const contextAfter = unchangedBuffer.slice(-CONTEXT_SIZE);
                    const hidden = unchangedBuffer.slice(CONTEXT_SIZE, -CONTEXT_SIZE);

                    lines.push(...contextBefore.map(f => ({ file: f, type: 'context' })));

                    if (hidden.length > 0) {
                        lines.push({
                            type: 'collapse',
                            count: hidden.length,
                            files: hidden
                        });
                    }

                    lines.push(...contextAfter.map(f => ({ file: f, type: 'context' })));
                } else {
                    // Small gap - show all as context
                    lines.push(...unchangedBuffer.map(f => ({ file: f, type: 'context' })));
                }

                unchangedBuffer = [];
            }

            // Add the changed line
            lines.push({
                file,
                type: isRemoved ? 'removed' : 'added'
            });

            lastChangeIndex = index;
        } else {
            // Unchanged file - add to buffer
            unchangedBuffer.push(file);
        }
    });

    // Handle any remaining unchanged files at the end
    if (unchangedBuffer.length > 0) {
        if (unchangedBuffer.length > CONTEXT_SIZE) {
            const context = unchangedBuffer.slice(0, CONTEXT_SIZE);
            const hidden = unchangedBuffer.slice(CONTEXT_SIZE);

            lines.push(...context.map(f => ({ file: f, type: 'context' })));

            if (hidden.length > 0) {
                lines.push({
                    type: 'collapse',
                    count: hidden.length,
                    files: hidden
                });
            }
        } else {
            lines.push(...unchangedBuffer.map(f => ({ file: f, type: 'context' })));
        }
    }

    // Render the diff
    let html = '';
    let collapseId = 0;

    lines.forEach(line => {
        if (line.type === 'collapse') {
            const id = `collapse-${collapseId++}`;
            html += `
                <button class="diff-collapse" data-target="${id}">
                    ${line.count} unchanged file${line.count !== 1 ? 's' : ''}
                </button>
                <div class="diff-section" id="${id}">
                    ${line.files.map(f =>
                        `<div class="diff-line unchanged">${escapeHtml(f)}</div>`
                    ).join('')}
                </div>
            `;
        } else {
            const prefix = line.type === 'removed' ? '- ' : line.type === 'added' ? '+ ' : '  ';
            html += `<div class="diff-line ${line.type}">${prefix}${escapeHtml(line.file)}</div>`;
        }
    });

    viewer.innerHTML = html;

    // Setup collapse/expand handlers
    viewer.querySelectorAll('.diff-collapse').forEach(button => {
        button.addEventListener('click', () => {
            const targetId = button.dataset.target;
            const section = document.getElementById(targetId);
            const isExpanded = section.classList.contains('expanded');

            if (isExpanded) {
                section.classList.remove('expanded');
                button.classList.remove('expanded');
            } else {
                section.classList.add('expanded');
                button.classList.add('expanded');
            }
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
                `../data/datasets/${datasetId}/snapshots/${latestSnapshot}/files.json`
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
