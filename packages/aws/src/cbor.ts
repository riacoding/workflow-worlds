/**
 * CBOR encode/decode for spec-version-3 payload fields.
 *
 * Mirrors the reference @workflow/world-postgres implementation: run/step/hook/
 * event payload fields (input, output, executionContext, error, metadata,
 * eventData) are stored as raw CBOR bytes rather than JSON. cbor-x handles
 * Date and Uint8Array natively (CBOR tag 1 and byte strings respectively), so
 * no extra tagging is needed for the fields that go through this codec.
 */

import { decode, encode } from 'cbor-x';

export function cborEncode(value: unknown): Uint8Array | undefined {
  if (value === undefined) return undefined;
  return encode(value);
}

export function cborDecode<T>(value: Uint8Array | undefined): T | undefined {
  if (value === undefined) return undefined;
  return decode(value) as T;
}
