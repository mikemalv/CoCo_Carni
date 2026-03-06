const cache = new Map<string, { data: unknown; timestamp: number }>();
const CACHE_TTL = 5 * 60 * 1000;
const inflight = new Map<string, Promise<unknown>>();

export async function fetchWithCache<T>(url: string): Promise<T> {
  const cached = cache.get(url);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.data as T;
  }
  
  const existing = inflight.get(url);
  if (existing) return existing as Promise<T>;
  
  const promise = fetch(url)
    .then(async (res) => {
      if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);
      const data = await res.json();
      cache.set(url, { data, timestamp: Date.now() });
      return data;
    })
    .finally(() => inflight.delete(url));
  
  inflight.set(url, promise);
  return promise as Promise<T>;
}

export function prefetchAllData() {
  const endpoints = ["/api/members", "/api/slots?ship=All%20Ships", "/api/ml", "/api/policies"];
  endpoints.forEach((url) => fetchWithCache(url).catch(() => {}));
}

export function invalidateCache(url?: string) {
  if (url) cache.delete(url);
  else cache.clear();
}
