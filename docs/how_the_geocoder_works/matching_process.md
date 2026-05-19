---
description: An graphical overview of how the Geocoder works
icon: lucide/network
---

# The Matching Process

## The Matching Pipeline
```mermaid
flowchart LR

A[Input] --> B{Philly address?}
B -- Yes --> C[Address File]
B -- No --> D[TomTom]
C -- No Match --> E[AIS]
E -- No Match --> D
D -- Match --> F[AIS Re-match]
```

## Match Types
<div class="grid cards" markdown>

- <span style="color: green">:lucide-circle-check:</span> **Address File Match**

    Matched against the address file directly.

    Returns coordinates + all requested enrichment fields.

- <span style="color: green">:lucide-circle-check:</span> **Full AIS Match**

    Matched against AIS directly.

    Returns coordinates + all requested enrichment fields.

- <span style="color: orange">:lucide-circle-alert:</span> **AIS Intersection Match**

    Matched to an intersection in AIS.

    Returns coordinates + a partial list of enrichment fields.

- <span style="color: orange">:lucide-circle-alert:</span> **AIS Service Area Match**

    Failed to match to an exact maddress in AIS, but matched to a service area via a coordinate lookup.

    Returns coordinates + a partial list of enrichment fields.

- <span style="color: #FF4433">:lucide-circle-alert:</span> **TomTom Match**

    Matched to TomTom, but failed to match to AIS. 

    Returns coordinates, but no enrichment fields.

- <span style="color: red">:lucide-circle-x:</span> **No Match** </span>

    Failed to match to the address file, AIS, or TomTom.

    Returns nothing.

</div>