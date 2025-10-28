// src/utils/api.js
export function getAuthHeader() {
  const u = localStorage.getItem("u");
  const p = localStorage.getItem("p");
  if (!u || !p) return {};
  return { Authorization: "Basic " + btoa(`${u}:${p}`) };
}

export async function apiFetch(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}), ...getAuthHeader() },
  });
  if (res.status === 401) {
    localStorage.removeItem("u");
    localStorage.removeItem("p");
    throw new Error("Unauthorized");
  }
  return res;
}
