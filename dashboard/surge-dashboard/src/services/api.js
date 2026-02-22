const getApiBase = () => {
  const baseTag = document.querySelector("base");
  if (baseTag && baseTag.href) {
    try {
      const url = new URL(baseTag.href);
      return url.pathname.replace(/\/$/, "") + "/api";
    } catch {
      const match = baseTag.href.match(/https?:\/\/[^/]+(\/.*)/);
      if (match) {
        return match[1].replace(/\/$/, "") + "/api";
      }
      return "/api";
    }
  }
  return "/api";
};

const API_BASE = getApiBase();

export { API_BASE };

// Helper to safely parse JSON responses
async function parseJSONResponse(res) {
  const contentType = res.headers.get("content-type") || "";

  // If Content-Type is explicitly application/json, parse directly
  if (contentType.includes("application/json")) {
    return res.json();
  }

  // Otherwise, read as text first to check what we got
  const text = await res.text();

  // If we get HTML, it likely means the backend server isn't running
  if (
    contentType.includes("text/html") ||
    text.trim().startsWith("<!doctype") ||
    text.trim().startsWith("<!DOCTYPE")
  ) {
    throw new Error(
      "Backend server not available. Please ensure the Surge server is running on port 8080.",
    );
  }

  // Try to parse as JSON even if Content-Type is not application/json
  // (some backends send JSON with text/plain or other content types)
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(
      `Failed to parse JSON response. Content-Type: ${contentType || "unknown"}. Response: ${text.substring(0, 100)}`,
    );
  }
}

export const api = {
  getQueues: async () => {
    const res = await fetch(`${API_BASE}/queues`);
    if (!res.ok)
      throw new Error(
        `Failed to fetch queues: ${res.status} ${res.statusText}`,
      );
    return parseJSONResponse(res);
  },

  getQueueStats: async (namespace, queue, { from, to } = {}) => {
    let url = `${API_BASE}/queue/stats?namespace=${namespace}&queue=${queue}`;
    if (from) url += `&from=${from}`;
    if (to) url += `&to=${to}`;
    const res = await fetch(url);
    if (!res.ok)
      throw new Error(`Failed to fetch stats: ${res.status} ${res.statusText}`);
    return parseJSONResponse(res);
  },

  getBatchQueueStats: async (namespace, { from, to } = {}) => {
    let url = `${API_BASE}/queue/stats/batch?namespace=${namespace}`;
    if (from) url += `&from=${from}`;
    if (to) url += `&to=${to}`;
    const res = await fetch(url);
    if (!res.ok)
      throw new Error(
        `Failed to fetch batch stats: ${res.status} ${res.statusText}`,
      );
    return parseJSONResponse(res);
  },

  getScheduledJobs: async (namespace, queue, offset = 0, limit = 10) => {
    const res = await fetch(
      `${API_BASE}/queue/scheduled?namespace=${namespace}&queue=${queue}&offset=${offset}&limit=${limit}`,
    );
    if (!res.ok)
      throw new Error(
        `Failed to fetch scheduled jobs: ${res.status} ${res.statusText}`,
      );
    return parseJSONResponse(res);
  },

  getDLQ: async (namespace, queue, offset = 0, limit = 10) => {
    const res = await fetch(
      `${API_BASE}/dlq?namespace=${namespace}&queue=${queue}&offset=${offset}&limit=${limit}`,
    );
    if (!res.ok)
      throw new Error(`Failed to fetch DLQ: ${res.status} ${res.statusText}`);
    return parseJSONResponse(res);
  },

  getNamespaces: async () => {
    const res = await fetch(`${API_BASE}/namespaces`);
    if (!res.ok)
      throw new Error(
        `Failed to fetch namespaces: ${res.status} ${res.statusText}`,
      );
    return parseJSONResponse(res);
  },

  pauseQueue: async (namespace, queue) => {
    const res = await fetch(
      `${API_BASE}/queue/pause?namespace=${namespace}&queue=${queue}`,
      { method: "POST" },
    );
    if (!res.ok)
      throw new Error(`Failed to pause queue: ${res.status} ${res.statusText}`);
  },

  resumeQueue: async (namespace, queue) => {
    const res = await fetch(
      `${API_BASE}/queue/resume?namespace=${namespace}&queue=${queue}`,
      { method: "POST" },
    );
    if (!res.ok)
      throw new Error(
        `Failed to resume queue: ${res.status} ${res.statusText}`,
      );
  },

  drainQueue: async (namespace, queue) => {
    const res = await fetch(
      `${API_BASE}/queue/drain?namespace=${namespace}&queue=${queue}`,
      { method: "POST" },
    );
    if (!res.ok)
      throw new Error(`Failed to drain queue: ${res.status} ${res.statusText}`);
    return parseJSONResponse(res);
  },

  getHandlers: async () => {
    const res = await fetch(`${API_BASE}/handlers`);
    if (!res.ok)
      throw new Error(
        `Failed to fetch handlers: ${res.status} ${res.statusText}`,
      );
    return parseJSONResponse(res);
  },

  getWorkers: async () => {
    const res = await fetch(`${API_BASE}/workers`);
    if (!res.ok)
      throw new Error(
        `Failed to fetch workers: ${res.status} ${res.statusText}`,
      );
    return parseJSONResponse(res);
  },

  retryJob: async (jobId) => {
    const res = await fetch(`${API_BASE}/dlq/retry?job_id=${jobId}`, {
      method: "POST",
    });
    if (!res.ok)
      throw new Error(`Failed to retry job: ${res.status} ${res.statusText}`);
    return parseJSONResponse(res);
  },
};
