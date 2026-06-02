# Skill: Multimodal Copy & Creative Brief Assistant

> **Capability**: Transforms historical performance data and product context into
> platform-ready ad copy variants and production-grade visual creative briefs.
> Used by the `generate_creative_campaign_brief` tool in the Operator agent.

---

## 1. Copywriting Frameworks

### 1.1 PAS — Problem · Agitate · Solution

Structure designed to activate pain-point awareness before presenting relief.

| Beat | Role | Tactics |
|------|------|---------|
| **Problem** | Name the friction | State the specific, relatable issue the persona faces. Avoid generic pain — use the language of the ICP. |
| **Agitate** | Deepen urgency | Amplify the cost of inaction. Make the status quo feel expensive, risky, or embarrassing. |
| **Solution** | Resolve with the product | Position the offer as the obvious, effortless fix. Lead the CTA into the resolution. |

**Example (SaaS B2B lead gen):**
> *Your team is still manually chasing MQL handoffs. Every hour in spreadsheets is a deal you didn't close. [Product] syncs intent signals to your CRM in real time — zero ops overhead.*

---

### 1.2 AIDA — Attention · Interest · Desire · Action

Classic funnel framework optimised for cold-audience display and social.

| Beat | Role | Tactics |
|------|------|---------|
| **Attention** | Pattern interrupt | Bold stat, provocative question, or visual hook word (Stop / Finally / Warning). |
| **Interest** | Context relevance | Connect the product to a known workflow or category the persona already cares about. |
| **Desire** | Proof + social signal | Quantified outcome, customer name/logo, or before/after transformation. |
| **Action** | Friction-free CTA | Single verb + low-commitment framing ("See how", "Get the report", "Start free"). |

**Example (paid media analytics tool):**
> *Stop guessing which campaigns actually drive revenue. [Product] gives marketing teams a single attribution view across every channel — no data team required. Trusted by 800+ B2B teams. See your pipeline attribution in 5 minutes.*

---

### 1.3 Benefit-Driven Direct Hook

Lead with the outcome, not the feature. Built for high-intent audiences (retargeting, branded search, warm lists).

| Component | Role | Tactics |
|-----------|------|---------|
| **Outcome headline** | State the result first | Quantify where possible: time saved, cost reduced, metric improved. |
| **Supporting proof** | One credibility signal | Customer stat, award, integration badge, or analyst mention. |
| **CTA** | Direct and specific | Match CTA specificity to funnel stage: "Book a demo" for bottom, "See the benchmark" for mid. |

**Example (enterprise SaaS):**
> *Cut media waste by 34%. [Product]'s AI attribution model re-allocates budget in real time — automatically. Trusted by 3 Fortune 500 marketing teams. Request a custom ROI analysis.*

---

## 2. Platform Copy Constraints

### 2.1 Google Responsive Search Ads (RSA)

| Field | Hard Limit | Best Practice |
|-------|-----------|---------------|
| Headline | **30 characters** | Write 12–15 headlines. Vary hook type (question, benefit, brand). No sentence case — Title Case for headlines. |
| Description | **90 characters** | Write 4 descriptions. One should reinforce the primary value prop; one should include a social proof signal. |
| URL path | 15 chars × 2 | Include primary keyword. |

> **Compliance check**: Any headline ≥ 31 chars must be rejected and rewritten. The tool enforces this before output.

---

### 2.2 LinkedIn Single Image / Sponsored Content

| Field | Hard Limit | Truncation Threshold | Best Practice |
|-------|-----------|----------------------|---------------|
| Primary text (intro text) | 600 chars | **150 chars** — truncated below the fold on desktop | Keep critical message and CTA within 150 chars. |
| Headline | 70 chars | 70 chars | Use title case. Lead with a quantified outcome or job-title-specific hook. |
| Description | 100 chars | 100 chars | Optional; use for secondary proof or context. |
| CTA button | Preset options | — | "Learn More" / "Download" / "Sign Up" / "Request Demo" |

> **LinkedIn context**: Primary text > 150 chars requires "...see more" click. Treat 150 chars as the effective limit for conversion copy. The remaining capacity (up to 600 chars) can carry longer context for organic dwell, but conversion-critical copy must land before truncation.

---

### 2.3 Meta (Facebook & Instagram) Ads

| Field | Recommended Max | Notes |
|-------|----------------|-------|
| Primary text | **125 chars** (feed) | Truncated to "... See more" at ~125 chars in feed. Stories/Reels: full text shown. |
| Headline | **27 chars** (feed link) | Below the creative in the link preview tile. Short and punchy. |
| Description | **27 chars** | Appears below the headline in the link preview. Reinforce the offer. |
| CTA button | Preset | "Learn More" / "Shop Now" / "Sign Up" / "Book Now" / "Get Quote" |

> **Meta creative note**: For feed ads, assume the primary text must stand alone — many users never read the headline. Lead with the hook in the first 125 chars.

---

### 2.4 TikTok In-Feed Ads

| Field | Hard Limit | Notes |
|-------|-----------|-------|
| Ad text | **100 characters** | Overlaid on video — keep scannable, one idea per line. Emojis permitted. |
| Display name | 40 chars | Brand or product name |
| CTA | Preset | "Learn More" / "Shop Now" / "Sign Up" |

> **TikTok note**: The ad text appears as a caption overlay, not a separate text block. Write for scan speed — one value prop, short words, optional single emoji as visual break.

---

## 3. Visual Creative Brief Schema

Every visual asset brief produced by this skill must include the following sections.
This schema maps directly to the `visual_creative_briefs` array in the tool's JSON output.

---

### 3.1 Visual Core Concept
The psychological hook or visual metaphor driving the asset.

- What emotion or cognitive state does the viewer enter in the first 1.5 seconds?
- What is the central visual metaphor (contrast, transformation, social proof moment, authority signal)?
- What does the viewer *see themselves doing* as a result of this ad?

---

### 3.2 Format & Aspect Ratios

Specify the exact placement target and its canonical aspect ratio.

| Placement Target | Format | Aspect Ratio | Duration |
|-----------------|--------|-------------|----------|
| Meta Reels / TikTok In-Feed | Short-form vertical video | **9:16** | 15–30 sec |
| LinkedIn / Meta Feed | Square image or video | **1:1** | static / ≤60 sec |
| Display / Programmatic | Horizontal banner | **16:9** | static / ≤30 sec |
| Stories (Meta / LinkedIn) | Full-screen vertical | **9:16** | static / ≤15 sec |
| Google Responsive Display | Flexible — provide 1:1 + 1.91:1 | both | static |

> Each brief must declare which placement(s) it targets and the production deliverable format.

---

### 3.3 Aesthetic Style & Tone Guidance

| Element | Description |
|---------|-------------|
| **Color palette** | Primary, secondary, and accent hex values. Note whether brand guidelines constrain this or creative latitude is available. |
| **Lighting vector** | Hard directional (authority, precision), soft diffuse (approachable, human), or high-contrast dramatic (disruptive, urgent). |
| **Composition style** | Minimalist negative space, dense social-proof collage, UX screen capture, or lifestyle/human-centered. |
| **Typography treatment** | Bold display weight for hooks; sentence case for body overlays. Recommend web-safe or brand font family. |
| **Tone markers** | Two or three adjectives that define the emotional register: "authoritative + urgent", "playful + premium", "clinical + reassuring". |

---

### 3.4 On-Screen Copy / Overlays

For video assets, specify copy by timestamp. For static assets, specify copy by layer/zone.

**Video format:**
| Timestamp | Layer | Copy | Style |
|-----------|-------|------|-------|
| 0:00–0:02 | Hook overlay | [exact text] | Bold, high contrast |
| 0:03–0:10 | Body copy | [exact text] | Regular weight |
| 0:11–0:14 | CTA card | [exact text + CTA button label] | Brand color BG |

**Static format:**
| Zone | Copy | Notes |
|------|------|-------|
| Top 20% | Hook headline | 2–5 words max |
| Mid | Value prop / proof | 1–2 lines |
| Bottom | CTA | Brand button style |

---

### 3.5 AI Image Generation Prompt

A production-grade prompt for text-to-image models (Midjourney, DALL·E 3, Stable Diffusion, Firefly).

**Structure:**
```
[Subject description] + [Action or state] + [Environment/setting] + [Lighting] +
[Camera angle/composition] + [Color palette] + [Style keywords] +
[Negative prompts if applicable] + [Technical quality flags]
```

**Quality flags to include where relevant:**
- `photorealistic`, `8K`, `sharp focus`, `studio quality`
- `shot on Sony A7R IV`, `f/1.8 bokeh` (for human-centered lifestyle)
- `isometric 3D`, `minimal flat design` (for product/UI-first)
- `cinematic color grade`, `ARRI Alexa` (for video stills)

**Example (B2B SaaS, attribution dashboard):**
> *A modern marketing analyst at a sleek standing desk, focused on a dual-monitor setup displaying colorful attribution funnel charts, soft north-light flooding from the left, shallow depth of field blurring the open-plan office behind them, brand-aligned navy and amber color accents, photorealistic, 8K, shot on Sony A7R IV at f/1.8, high-end tech company aesthetic, --ar 1:1 --no text logos watermarks*

---

## 4. Few-Shot Integration Protocol

When historical top-performers are available (from `get_top_performing_ads()`),
the generation prompt must include them as structural examples — not verbatim copy
to re-use, but pattern templates showing which:

1. **Hook constructions** drove clicks (question-hook vs. benefit-lead vs. stat-open)
2. **Value prop language** converted (specific metrics vs. category claims)
3. **CTA phrasing** closed action (verb specificity, commitment level)
4. **Format pairings** — which creative_format correlated with best CVR for this channel

The model should adapt these patterns for the new campaign's persona and value proposition
without plagiarising the historical copy strings directly.

---

## 5. Copy Quality Gates

Before returning any copy variant, validate:

| Check | Rule | Reject if |
|-------|------|-----------|
| Google RSA headline length | ≤ 30 chars | headline > 30 chars |
| LinkedIn primary text truncation | Key message + CTA within 150 chars | CTA appears after char 150 |
| Meta primary text truncation | Core hook within 125 chars | Hook truncated |
| TikTok ad text | ≤ 100 chars | text > 100 chars |
| Duplicate CTA | Each variant must have a distinct CTA verb | Two variants share identical CTA |
| Framework fidelity | PAS must include all 3 beats; AIDA all 4 | Missing structural beat |
