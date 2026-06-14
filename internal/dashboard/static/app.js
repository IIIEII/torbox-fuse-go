// app.js — TorBox Media Center Dashboard
// Connects to SSE endpoint, renders file cache visualization.

(function () {
  'use strict';

  const RECONNECT_DELAY = 2000;

  // DOM references
  const summaryEl = document.getElementById('summary');
  const activeEl = document.getElementById('active-files');
  const closedEl = document.getElementById('closed-files');
  const hiddenEl = document.getElementById('hidden-downloads');
  const activeCountEl = document.getElementById('active-count');
  const closedCountEl = document.getElementById('closed-count');
  const hiddenCountEl = document.getElementById('hidden-count');
  const searchInput = document.getElementById('search');
  const statusDot = document.getElementById('status-dot');
  const statusText = document.getElementById('status-text');

  // State: previous snapshot for diffing
  let prevFiles = {};

  // Connect to SSE endpoint
  function connect() {
    statusDot.className = 'status-dot connecting';
    statusText.textContent = 'Connecting...';

    const es = new EventSource('/api/state');

    es.onopen = function () {
      statusDot.className = 'status-dot connected';
      statusText.textContent = 'Live';
    };

    es.onerror = function () {
      statusDot.className = 'status-dot disconnected';
      statusText.textContent = 'Disconnected — retrying...';
      es.close();
      setTimeout(connect, RECONNECT_DELAY);
    };

    es.onmessage = function (event) {
      try {
        var snap = JSON.parse(event.data);
        renderSnapshot(snap);
      } catch (e) {
        console.error('Failed to parse snapshot:', e);
      }
    };
  }

  // Render a full dashboard snapshot
  function renderSnapshot(snap) {
    renderSummary(snap.summary);
    renderFileList(activeEl, activeCountEl, snap.active, 'active');
    renderClosedList(closedEl, closedCountEl, snap.recently_closed);
    renderHiddenList(hiddenEl, hiddenCountEl, snap.hidden_downloads || []);
  }

  // Render summary bar
  function renderSummary(s) {
    var budgetPct = s.budget_slots_total > 0 ? Math.round(s.budget_slots_used / s.budget_slots_total * 100) : 0;
    var cachePct = s.cache_bytes_budget > 0 ? Math.round(s.cache_bytes_used / s.cache_bytes_budget * 100) : 0;
    summaryEl.innerHTML =
      '<div class="stat"><span class="stat-label">Budget</span>' +
      '<span class="stat-value">' + s.budget_slots_used + '/' + s.budget_slots_total + ' slots</span>' +
      '<div class="stat-bar"><div class="stat-bar-fill" style="width:' + budgetPct + '%"></div></div></div>' +
      '<div class="stat"><span class="stat-label">Cache</span>' +
      '<span class="stat-value">' + formatBytes(s.cache_bytes_used) + '/' + formatBytes(s.cache_bytes_budget) + '</span>' +
      '<div class="stat-bar"><div class="stat-bar-fill" style="width:' + cachePct + '%"></div></div></div>' +
      '<div class="stat"><span class="stat-label">Inflight</span>' +
      '<span class="stat-value">' + s.inflight_windows + ' windows</span></div>' +
      '<div class="stat"><span class="stat-label">Entries</span>' +
      '<span class="stat-value">' + s.cache_entries + '</span></div>';
  }

  // Render a list of active/cached files
  function renderFileList(container, countEl, files, section) {
    countEl.textContent = files ? files.length : '0';
    if (!files || files.length === 0) {
      container.innerHTML = '<div class="empty">No files</div>';
      return;
    }

    var filter = (searchInput.value || '').toLowerCase();
    var html = '';
    for (var i = 0; i < files.length; i++) {
      var f = files[i];
      if (filter && f.file_path.toLowerCase().indexOf(filter) < 0 && f.file_key.toLowerCase().indexOf(filter) < 0) {
        continue;
      }
      html += renderFileBar(f, section);
    }
    if (!html) {
      html = '<div class="empty">No matching files</div>';
    }
    container.innerHTML = html;
  }

  // Render a closed files list
  function renderClosedList(container, countEl, files) {
    countEl.textContent = files ? files.length : '0';
    if (!files || files.length === 0) {
      container.innerHTML = '<div class="empty">No recently closed files</div>';
      return;
    }

    var filter = (searchInput.value || '').toLowerCase();
    var html = '';
    for (var i = 0; i < files.length; i++) {
      var f = files[i];
      if (filter && (f.file_path || '').toLowerCase().indexOf(filter) < 0 && f.file_key.toLowerCase().indexOf(filter) < 0) {
        continue;
      }
      var ago = timeSince(new Date(f.closed_at));
      var path = f.file_path || f.file_key;
      html += '<div class="file-entry closed">' +
        '<div class="file-name" title="' + esc(f.file_key) + '">' + esc(path) + '</div>' +
        '<div class="file-meta">' + formatBytes(f.file_size) + ' · closed ' + ago + '</div>' +
        '</div>';
    }
    if (!html) {
      html = '<div class="empty">No matching files</div>';
    }
    container.innerHTML = html;
  }

  // Render a single file bar (the "колбаска")
  function renderFileBar(f, section) {
    var path = f.file_path || f.file_key;
    var size = f.file_size;

    // Build segments for the progress bar
    var segments = buildSegments(f, size);

    // Calculate cache coverage percentage
    var cachedPct = size > 0 ? computeCachePercent(segments, size) : 0;

    var meta = '';
    meta += formatBytes(size);
    if (cachedPct > 0) {
      meta += ' · ' + cachedPct.toFixed(0) + '% cached';
    }
    if (f.pattern) {
      meta += ' · <span class="pattern pattern-' + esc(f.pattern) + '">' + esc(f.pattern) + '</span>';
    }
    if (f.priority === 1) {
      meta += ' · <span class="priority-high">HIGH</span>';
    } else if (f.priority === 0 && f.pattern !== 'idle') {
      meta += ' · <span class="priority-low">LOW</span>';
    }

    var barHtml = renderBar(segments, size);

    // Read position markers
    var markers = '';
    if (f.read_offsets && f.read_offsets.length > 0) {
      for (var i = 0; i < f.read_offsets.length; i++) {
        var pct = size > 0 ? (f.read_offsets[i] / size * 100).toFixed(1) : 0;
        markers += '<div class="read-marker" style="left:' + pct + '%" title="Read @ ' + formatBytes(f.read_offsets[i]) + '"></div>';
      }
    }

    return '<div class="file-entry ' + section + '">' +
      '<div class="file-name" title="' + esc(f.file_key) + '">' + esc(path) + '</div>' +
      '<div class="file-bar-container">' + barHtml + markers + '</div>' +
      '<div class="file-meta">' + meta + '</div>' +
      '</div>';
  }

  // Build colored segments from file snapshot data
  function buildSegments(f, fileSize) {
    var segments = [];
    if (fileSize <= 0) return segments;

    // Add cached blocks (green = high priority, yellow = low priority)
    if (f.cached_blocks) {
      for (var i = 0; i < f.cached_blocks.length; i++) {
        var b = f.cached_blocks[i];
        segments.push({
          start: b.start,
          end: b.end,
          type: b.priority === 1 ? 'cached-high' : 'cached-low'
        });
      }
    }

    // Add inflight windows.
    // ready_to is a relative offset within the window buffer (0 = start of window),
    // so absolute positions are computed as w.start + w.ready_to.
    // Done windows are shown as cached segments (using their priority color)
    // since the data is fully downloaded but may not yet appear in cached_blocks
    // due to cache eviction timing. Active windows show progress + pending.
    var windowSize = 16 * 1024 * 1024; // must match reader.go windowSize
    if (f.inflight) {
      for (var i = 0; i < f.inflight.length; i++) {
        var w = f.inflight[i];
        // Compute absolute end of this window, clamped to file size
        var windowEnd = f.file_size > 0 ? Math.min(w.start + windowSize, f.file_size) : w.start + windowSize;
        // Absolute position of downloaded progress within the window
        var progressEnd = w.start + w.ready_to;

        if (w.done) {
          // Completed download — show as cached using its priority color.
          // This covers ranges that may be missing from cached_blocks
          // due to eviction or merge timing.
          segments.push({
            start: w.start,
            end: progressEnd > w.start ? progressEnd : w.start,
            type: w.priority === 1 ? 'cached-high' : 'cached-low'
          });
          continue;
        }
        // Active download — show progress portion in blue
        if (progressEnd > w.start) {
          segments.push({
            start: w.start,
            end: progressEnd,
            type: 'inflight-progress'
          });
        }
        // Pending portion (not yet downloaded)
        if (progressEnd < windowEnd) {
          segments.push({
            start: progressEnd,
            end: windowEnd,
            type: 'inflight-pending'
          });
        }
      }
    }

    // Sort segments by start offset
    segments.sort(function (a, b) { return a.start - b.start; });
    return segments;
  }

  // Compute what percentage of the file is cached (done inflight + cached blocks).
  // Inflight-progress and inflight-pending segments are NOT counted.
  function computeCachePercent(segments, fileSize) {
    if (fileSize <= 0 || !segments || segments.length === 0) return 0;
    var cachedBytes = 0;
    for (var i = 0; i < segments.length; i++) {
      var s = segments[i];
      if (s.type === 'cached-high' || s.type === 'cached-low') {
        cachedBytes += s.end - s.start;
      }
    }
    return cachedBytes / fileSize * 100;
  }

  // Render a progress bar from segments
  function renderBar(segments, fileSize) {
    if (fileSize <= 0) return '<div class="file-bar"></div>';

    var html = '<div class="file-bar">';
    for (var i = 0; i < segments.length; i++) {
      var s = segments[i];
      var left = (s.start / fileSize * 100).toFixed(2);
      var width = Math.max(((s.end - s.start) / fileSize * 100), 0.3).toFixed(2);
      var cls = 'bar-segment ' + s.type;
      html += '<div class="' + cls + '" style="left:' + left + '%;width:' + width + '%" title="' + formatBytes(s.start) + '-' + formatBytes(s.end) + '"></div>';
    }
    html += '</div>';
    return html;
  }

  // Utility: format bytes
  function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    var units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    var i = Math.floor(Math.log(Math.max(1, bytes)) / Math.log(1024));
    if (i >= units.length) i = units.length - 1;
    return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
  }

  // Utility: time since
  function timeSince(date) {
    var seconds = Math.floor((Date.now() - date.getTime()) / 1000);
    if (seconds < 60) return seconds + 's ago';
    var minutes = Math.floor(seconds / 60);
    if (minutes < 60) return minutes + 'm ago';
    var hours = Math.floor(minutes / 60);
    return hours + 'h ago';
  }

  // Utility: HTML escape
  function esc(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  // Render hidden downloads list
  function renderHiddenList(container, countEl, downloads) {
    countEl.textContent = downloads ? downloads.length : '0';
    if (!downloads || downloads.length === 0) {
      container.innerHTML = '<div class="empty">No hidden downloads</div>';
      return;
    }

    var filter = (searchInput.value || '').toLowerCase();
    var html = '';
    for (var i = 0; i < downloads.length; i++) {
      var d = downloads[i];
      if (filter && d.download_name.toLowerCase().indexOf(filter) < 0 && d.download_kind.toLowerCase().indexOf(filter) < 0) {
        continue;
      }
      var statusBadge = d.fully_hidden
        ? '<span class="hidden-badge fully-hidden">All files hidden</span>'
        : '<span class="hidden-badge partial">' + d.hidden_count + '/' + d.total_count + ' hidden</span>';
      html += '<div class="file-entry hidden-entry">' +
        '<div class="file-name">' + esc(d.download_name) + ' ' + statusBadge + '</div>' +
        '<div class="file-meta">' + esc(d.download_kind) + ' · ' + d.download_id + ' · ' + formatBytes(d.total_size) + '</div>' +
        '<div class="hidden-actions">' +
        '<button class="btn-unhide" onclick="window._unhide(\'' + esc(d.download_kind) + '\',\'' + esc(d.download_id) + '\')">Unhide</button>' +
        (d.fully_hidden ? '<button class="btn-delete" onclick="window._delete(\'' + esc(d.download_kind) + '\',\'' + esc(d.download_id) + '\')">Delete from TorBox</button>' : '') +
        '</div></div>';
    }
    if (!html) {
      html = '<div class="empty">No matching downloads</div>';
    }
    container.innerHTML = html;
  }

  // Unhide a download's files
  window._unhide = function (kind, id) {
    fetch('/api/unhide', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ download_kind: kind, download_id: id })
    }).then(function (r) {
      if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
      return r.json();
    }).then(function () {
      // Snapshot will auto-refresh via SSE
    }).catch(function (e) {
      alert('Failed to unhide: ' + e.message);
    });
  };

  // Force-delete a download from TorBox
  window._delete = function (kind, id) {
    if (!confirm('Delete this download from TorBox? This cannot be undone.')) return;
    fetch('/api/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ download_kind: kind, download_id: id })
    }).then(function (r) {
      if (!r.ok) return r.text().then(function (t) { throw new Error(t); });
      return r.json();
    }).then(function () {
      // Snapshot will auto-refresh via SSE
    }).catch(function (e) {
      alert('Failed to delete: ' + e.message);
    });
  };

  // Search filter handler
  searchInput.addEventListener('input', function () {
    // Re-render on next snapshot — the filter is read during render
  });

  // Start connection
  connect();
})();