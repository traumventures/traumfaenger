// Text preparation shared by the recorders: everything a frame or a context
// field goes through before it leaves the process.
import { LIMITS } from "@traumfaenger/api";

const encoder = new TextEncoder();

export function byteLength(text: string): number {
    return encoder.encode(text).byteLength;
}

export interface Clipped {
    text: string;
    truncated: boolean;
}

/** Clips to `maxBytes` of UTF-8 without splitting a code point. */
export function clipBytes(text: string, maxBytes: number): Clipped {
    if (text.length * 3 <= maxBytes) return { text, truncated: false };
    const bytes = encoder.encode(text);
    if (bytes.byteLength <= maxBytes) return { text, truncated: false };
    let end = Math.max(maxBytes, 0);
    while (end > 0 && ((bytes[end] ?? 0) & 0xc0) === 0x80) end--;
    return { text: new TextDecoder().decode(bytes.subarray(0, end)), truncated: true };
}

/** A UTF-16 surrogate without its partner. */
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g;

/**
 * Replaces lone UTF-16 surrogates, which strict UTF-8 and JSON decoders
 * reject. Same as `String.prototype.toWellFormed`, which hosts compiling
 * against an ES2023 lib cannot name.
 */
export function wellFormed(text: string): string {
    return text.replace(LONE_SURROGATE, "\uFFFD");
}

/** Inline base64 data of at least 1 KiB, as in `data:image/png;base64,...`. */
const DATA_URL = /data:([\w.+-]+\/[\w.+-]+);base64,[A-Za-z0-9+/]{1024,}={0,2}/g;
/** A JSON string that is nothing but base64 of at least 1 KiB, as realtime audio chunks are. */
const BASE64_STRING = /"([A-Za-z0-9+/]{1024,}={0,2})"/g;

function decodedBytes(base64Length: number): number {
    return Math.floor((base64Length * 3) / 4);
}

/**
 * Prepares frame data: inline base64 payloads (images, files, audio) become a
 * short placeholder, lone surrogates are replaced. Everything else stays
 * byte for byte.
 */
export function prepareData(text: string): string {
    let prepared = text;
    if (prepared.includes(";base64,")) {
        prepared = prepared.replace(DATA_URL, (match, mime: string) => {
            return `[traumfaenger: omitted ${mime} data, ${decodedBytes(match.length - match.indexOf(",") - 1)} bytes]`;
        });
    }
    if (prepared.length >= 1_026) {
        prepared = prepared.replace(
            BASE64_STRING,
            (_match, payload: string) => `"[traumfaenger: omitted base64, ${decodedBytes(payload.length)} bytes]"`
        );
    }
    return wellFormed(prepared);
}

/** Placeholder for binary data that is not recorded. */
export function binaryPlaceholder(kind: string, bytes: number | undefined): string {
    return `[traumfaenger: unrecorded ${kind}${bytes === undefined ? "" : `, ${bytes} bytes`}]`;
}

/** A single line of at most `maxNameLen` bytes, or undefined when empty. */
export function cleanLine(value: unknown): { value?: string; clipped: boolean } {
    if (value === undefined || value === null) return { clipped: false };
    const line = wellFormed(String(value)).replace(/\p{Cc}+/gu, " ").trim();
    if (line === "") return { clipped: true };
    const clipped = clipBytes(line, LIMITS.maxNameLen);
    return { value: clipped.text.trim(), clipped: clipped.truncated };
}

export function isLine(value: unknown): boolean {
    const cleaned = cleanLine(value);
    return cleaned.value !== undefined && !cleaned.clipped && cleaned.value === value;
}

const METADATA_KEY = new RegExp(`^[A-Za-z0-9_.-]{1,${LIMITS.maxMetadataKeyLen}}$`);

export function cleanMetadata(metadata: Record<string, unknown> | undefined): {
    value?: Record<string, string>;
    clipped: boolean;
} {
    if (metadata === undefined || metadata === null) return { clipped: false };
    const out: Record<string, string> = {};
    let count = 0;
    let clipped = false;
    for (const [key, raw] of Object.entries(metadata)) {
        if (raw === undefined || raw === null) continue;
        if (!METADATA_KEY.test(key) || count >= LIMITS.maxMetadataEntries) {
            clipped = true;
            continue;
        }
        const value = clipBytes(wellFormed(String(raw)).replaceAll("\0", ""), LIMITS.maxMetadataValueLen);
        clipped ||= value.truncated;
        out[key] = value.text;
        count++;
    }
    return count === 0 ? { clipped } : { value: out, clipped };
}

/** Absolute http(s)/ws(s) URL without query string, fragment and credentials, which can carry keys. */
export function cleanUrl(raw: string): string | undefined {
    let url: URL;
    try {
        url = new URL(raw);
    } catch {
        return undefined;
    }
    if (!["https:", "http:", "wss:", "ws:"].includes(url.protocol)) return undefined;
    url.search = "";
    url.hash = "";
    url.username = "";
    url.password = "";
    return url.href.slice(0, LIMITS.maxUrlLen);
}

export function describe(error: unknown): string {
    let text: string;
    try {
        text = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    } catch {
        text = "unknown error";
    }
    text = wellFormed(text).trim() || "unknown error";
    return clipBytes(text, LIMITS.maxErrorLen).text;
}

/** Splits an SSE body into its events, each verbatim without the separating blank line. */
export function splitEvents(text: string): string[] {
    const splitter = new SseSplitter();
    return [...splitter.push(text), ...splitter.end()];
}

/**
 * Splits a stream of SSE text into events as they complete. The buffer is kept
 * normalized (LF only) and only new text is scanned, so one large event that
 * arrives in many chunks costs linear time.
 */
export class SseSplitter {
    #buffer = "";
    /** A trailing CR that may be the first half of a CRLF split across chunks. */
    #heldCr = false;

    /** Returns the events completed by this chunk. */
    push(chunk: string): string[] {
        let text = (this.#heldCr ? "\r" : "") + chunk;
        this.#heldCr = text.endsWith("\r");
        if (this.#heldCr) text = text.slice(0, -1);
        // A boundary can start in the last character of the old buffer.
        let from = Math.max(this.#buffer.length - 1, 0);
        this.#buffer += text.replace(/\r\n?/g, "\n");
        const events: string[] = [];
        let start = 0;
        for (;;) {
            const boundary = this.#buffer.indexOf("\n\n", from);
            if (boundary === -1) break;
            this.#emit(this.#buffer.slice(start, boundary), events);
            start = boundary + 2;
            from = start;
        }
        if (start > 0) this.#buffer = this.#buffer.slice(start);
        return events;
    }

    /** Returns the incomplete last event, if any. */
    end(): string[] {
        const events: string[] = [];
        this.#emit(this.#buffer, events);
        this.#buffer = "";
        this.#heldCr = false;
        return events;
    }

    #emit(event: string, events: string[]): void {
        // Extra blank lines between events are separators, not content.
        const trimmed = event.replace(/^\n+|\n+$/g, "");
        if (trimmed.trim() !== "") events.push(trimmed);
    }
}
