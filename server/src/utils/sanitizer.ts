/**
 * Sanitizes a filename to ensure it is safe for object storage consistency.
 * Rules:
 * - Basename only (no path traversal)
 * - Alphanumeric, underscore, hyphen, dot only
 * - Whitespace replaced by underscore
 * - Lowercase
 * - Max 128 characters
 * - Fallback to 'file' if empty
 */
export function sanitizeFilename(name: string | undefined | null): string {
  if (!name) return "file";

  // Take only the basename (handle both / and \ per request, though typical internal path is /)
  // We use a regex split to be safe against both separators
  const parts = name.split(/[\\/]/);
  const base = parts.pop() || "";

  if (!base) return "file";

  // Replace whitespace with underscore
  let s = base.replace(/\s+/g, "_");

  // Remove non-allowed characters (keep A-Z, a-z, 0-9, ., _, -)
  s = s.replace(/[^A-Za-z0-9._-]/g, "");

  // Convert to lowercase
  s = s.toLowerCase();

  // Trim length to 128 chars
  if (s.length > 128) {
    // Ideally we preserve extension, but simple truncation as per specific req is: "potong sisanya"
    // To be nicer, we could try to preserve extension, but strictly following spec:
    s = s.slice(0, 128);
  }

  // Fallback if empty after sanitization
  if (!s) return "file";

  return s;
}
