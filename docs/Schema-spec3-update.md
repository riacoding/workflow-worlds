# AWS World 4.2.1 – Event Persistence (Spec Version 3)

## Background

The AWS World is being upgraded from `@workflow/world@4.1.0-beta.2` to `4.2.1`.

The implementation already passes the official world tests on the earlier version. The goal is **not** to redesign the persistence model, but to bring the event storage implementation into conformance with the Workflow 4.2 specification so it is compatible with the official Workflow Inspector and future implementations.

The Postgres World is the reference implementation.

---

# Key Discovery

Workflow **specVersion = 3** changed event persistence.

Earlier versions stored:

```text
payload (JSON)
```

Spec version 3 stores:

```text
payload_cbor (binary CBOR)
```

The legacy JSON payload is intentionally `NULL` for new events.

Example schema from Postgres:

```
payload        jsonb   (NULL for specVersion 3)
payload_cbor   bytea
spec_version   integer
```

---

# Important Architecture

There are **two serialization layers**.

## Outer layer (owned by the World)

The World persists the event payload as CBOR.

Example:

```ts
{
    stepName,
    workflowName,
    input
}
```

This entire object is encoded as CBOR.

---

## Inner layer (owned by Workflow SDK)

Fields such as:

```
input
result
output
metadata
payload
```

are already serialized by the Workflow SDK.

These are NOT JSON.

They are serialized JavaScript values (currently devalue format wrapped in CBOR Tag 64).

The World must treat these values as opaque binary payloads.

Do NOT:

* stringify them
* JSON encode them
* decode/re-encode them
* convert to hex/base64

Persist them unchanged.

---

# Event Mapping

Current API event shape remains unchanged:

```ts
event.eventData
```

Persistence changes only.

Legacy:

```
payload = eventData
```

SpecVersion 3:

```
payloadCbor = CBOR.encode(eventData)
payload = null
```

Reading performs the reverse:

```
eventData = CBOR.decode(payloadCbor)
```

The Workflow runtime will then decode the nested serialized values.

---

# Event Types

The payload varies by event type.

Examples:

```
run_created
    deploymentId
    workflowName
    input
    executionContext

step_created
    stepName
    workflowName
    input

step_started
    stepName
    workflowName
    attempt

step_completed
    stepName
    workflowName
    result
```

Do not assume a single payload shape.

Use the event schema already defined by `@workflow/world`.

---

# DynamoDB Changes

Store canonical payload as a Binary attribute.

Conceptually:

```ts
payloadCbor: Binary
specVersion: number
```

Do not store CBOR as:

* string
* JSON
* base64
* hex

Store the raw bytes.

---

# Goal

The AWS World should produce the same logical persisted events as the Postgres World.

The Workflow Inspector should be able to read AWS-generated events without modification.

The official Workflow tests remain the source of truth.

---

# Success Criteria

* Store eventData as CBOR for specVersion 3.
* Preserve nested serialized values exactly.
* Decode CBOR back into eventData when reading.
* Existing event APIs remain unchanged.
* Official Workflow contract tests pass.
* Workflow Inspector displays AWS events identically to the Postgres World.


# example from Postgres
[
  {
    "id": "wevt_01KXYMPPT2X6H8XZ8AK3NNHKQ1",
    "type": "run_created",
    "correlation_id": null,
    "created_at": "2026-07-20 02:11:58.402991",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB900046C6465706C6F796D656E74496468706F7374677265736C776F726B666C6F774E616D657837776F726B666C6F772F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F68616E646C65557365725369676E757065696E707574D84058286465766C5B5B315D2C224445504C4F59504F53544752455340726961636F64696E672E636F6D225D70657865637574696F6E436F6E74657874B900036C747261636543617272696572B9000073776F726B666C6F77436F726556657273696F6E65342E362E30686665617475726573B900016A656E6372797074696F6EF4'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPPX5HF7DR0VD88NZQFHC",
    "type": "run_started",
    "correlation_id": null,
    "created_at": "2026-07-20 02:11:58.502704",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": null,
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPPYS28M2MPT6MY4E06MW",
    "type": "step_created",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6Q",
    "created_at": "2026-07-20 02:11:58.554760",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000368737465704E616D657830737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F777269746550726F67726573736C776F726B666C6F774E616D657837776F726B666C6F772F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F68616E646C65557365725369676E757065696E707574D84058456465766C5B7B2261726773223A312C22636C6F7375726556617273223A2D312C227468697356616C223A2D317D2C5B325D2C225374617274696E67207461736B2E2E2E225D'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPQ863NNPEJFP8H6AV7CX",
    "type": "step_started",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6Q",
    "created_at": "2026-07-20 02:11:58.851713",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000168737465704E616D657830737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F777269746550726F6772657373'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPQ8GNRGG5R4NA698FRNP",
    "type": "step_completed",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6Q",
    "created_at": "2026-07-20 02:11:58.865620",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000368737465704E616D657830737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F777269746550726F67726573736C776F726B666C6F774E616D657837776F726B666C6F772F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F68616E646C65557365725369676E757066726573756C74D840466465766C2D31'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPQ9674FBX0HYV61K17R6",
    "type": "step_created",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6R",
    "created_at": "2026-07-20 02:11:58.887686",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000368737465704E616D65782D737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F637265617465557365726C776F726B666C6F774E616D657837776F726B666C6F772F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F68616E646C65557365725369676E757065696E707574D84058516465766C5B7B2261726773223A312C22636C6F7375726556617273223A2D312C227468697356616C223A2D317D2C5B325D2C224445504C4F59504F53544752455340726961636F64696E672E636F6D225D'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPQAGYQMYMZHCWC9NFSYT",
    "type": "step_started",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6R",
    "created_at": "2026-07-20 02:11:58.926168",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000168737465704E616D65782D737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F63726561746555736572'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPQAQE57EC82R3RVTK7XS",
    "type": "step_completed",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6R",
    "created_at": "2026-07-20 02:11:58.936124",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000368737465704E616D65782D737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F637265617465557365726C776F726B666C6F774E616D657837776F726B666C6F772F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F68616E646C65557365725369676E757066726573756C74D840585E6465766C5B7B226964223A312C22656D61696C223A327D2C2233376135356562612D643862302D343131332D616666642D643263353561373361333733222C224445504C4F59504F53544752455340726961636F64696E672E636F6D225D'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPQB7WJMVAM5BGGXW14C8",
    "type": "step_created",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6S",
    "created_at": "2026-07-20 02:11:58.952969",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000368737465704E616D657830737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F777269746550726F67726573736C776F726B666C6F774E616D657837776F726B666C6F772F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F68616E646C65557365725369676E757065696E707574D84058676465766C5B7B2261726773223A312C22636C6F7375726556617273223A2D312C227468697356616C223A2D317D2C5B325D2C225573657220637265617465643A2033376135356562612D643862302D343131332D616666642D643263353561373361333733225D'",
    "spec_version": 3
  },
  {
    "id": "wevt_01KXYMPQCJ6MA3F76WY46NKR06",
    "type": "step_started",
    "correlation_id": "step_01KXYMPPWZ5V81Q6TYZ4X75N6S",
    "created_at": "2026-07-20 02:11:58.991197",
    "run_id": "wrun_01KXYMPPSB847AGQM2TAPTXSTK",
    "payload": null,
    "payload_cbor": "E'\\\\xB9000168737465704E616D657830737465702F2F2E2F7372632F776F726B666C6F77732F757365722D7369676E75702F2F777269746550726F6772657373'",
    "spec_version": 3
  }
]
