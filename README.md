# OpenXeroth

The public [OpenXeroth website](https://open.xeroth.ai): NatureCam, Hosana, AI zoomies, acoustic re-identification research and the public Djuma bird feed.

The website is deployed as the Cloudflare Worker `xeroth-open`, with static assets in `site/`. It does not serve pages, media or API traffic through the Johannesburg VM. Its read-only API fetches the fixed Djuma station from BirdNET-Cloud directly. Upstream audio acquisition continues to use the existing NatureCam infrastructure; this website does not change the camera, stream or relay.

## Develop and verify

Requires Node 22+, npm and Python 3.12+.

```bash
npm ci
npm run build
npm test
npm run dev
```

Open `http://localhost:8787`. `tools/build_site.py` generates the HTML; edit it instead of generated pages. Styles and browser logic live in `site/assets/`.

## Publish

After the PR has passed CI and merged, authenticate Wrangler in the authorised Cloudflare account and run `npm ci && npm run build && npm run deploy` from that exact commit. `wrangler.jsonc` binds only `open.xeroth.ai`. The old GitHub Pages configuration is legacy and is not the current serving origin.

The deployment before this redesign was Cloudflare version `edfdd0b2-fa4e-4455-8a7d-1ee9b14e2167`. Keep the actual deployment receipt for each update; Wrangler deployments/rollback can select a previous version. Deployment is intentionally manual: no personal Cloudflare token is stored in public CI.

## Bird feed

Public routes: `/djuma-birds/`, `/djuma-birds/species/?name=COMMON_NAME`, `/djuma-birds/call/?id=UUID`. The fixed-station API is `/api/birds/`. It supports bounded, normalised read-only queries and caches sanitised metadata for 60 seconds. It never returns signed media URLs or private upstream fields. Browsers refresh every minute while visible and idle; latest recording time and feed refresh time are distinct. Range-flagged detections are included. Provider summary counts can differ from the inclusive feed.

`PUBLIC_BIRD_MEDIA` is set to `true`: Xeroth confirmed Djuma authorisation for public display of these recordings on 2 October 2026. Audio and spectrograms are fetched server-side from the exact Djuma bucket/object namespace, with no redirects; callers cannot specify a destination URL. Unavailable/expired media does not remove the detection record. No bucket-wide public permissions are required.

Unsigned objects in the private GCS recording bucket require a dedicated service account. Store its JSON key as the Cloudflare secret `GCS_SERVICE_ACCOUNT` (never a repository file or public environment variable). Grant only Storage Object Viewer on the Djuma `birdnet/` prefix. The worker exchanges a signed assertion with Google's fixed OAuth endpoint for a short-lived, read-only storage token; it never forwards credentials to BirdNET-Cloud or a browser. Missing credentials or storage access failures return a temporary-unavailability response. Media responses are cached for five minutes; byte-range playback is supported. Historical BirdNET-hosted media is accessible only with a station-issued signed call link.

The feed, species and call views include Wikimedia reference photographs and file-page attribution, Wikipedia species links, eBird codes and occurrence values, range flags, model scores, recording times, inline playback and enlarged spectrograms. Feed controls support 25/50/100/200 rows, confidence thresholds and historical pagination. Species browsing supports today, seven days, thirty days and all time. The feed also shows daily and hourly summaries and the station's published configuration; each species has an hourly chart of its latest 1,000 detections, explicitly labelled as a sample. All times are SAST. These are machine species suggestions, not ground-truth species or individual identities.

Species pages use the provider's common-name filter. Call membership is checked against the Djuma media namespace or a signed call link issued from this station’s detection feed. `PUBLIC_CALL_KEY` is a random HMAC signing secret stored only in Cloudflare, never in Git. These public call links allow historical metadata to remain accessible even when its recording is unavailable. Rotating the key invalidates older historical call links; re-open the call from its species feed to obtain a current link. The media feature flag can disable playback independently of metadata access.

## Research

Research papers and articles are published under `/research/`. Each one is an entry in the `RESEARCH` list in `tools/build_site.py`, which also builds the `/research/` landing page from that list; an article's text is the Markdown file named in its entry, in `content/research/`. The paper's guide is at `/research/acoustic-re-id-research/`; the old address `/acoustic-re-id-research/` redirects there permanently through `site/_redirects`.

[Public research repository](https://github.com/OpenXeroth/acoustic-re-id-research). The paper is published as a [bioRxiv preprint, version 1](https://www.biorxiv.org/content/10.64898/2026.10.08.757537v1), DOI [10.64898/2026.10.08.757537](https://doi.org/10.64898/2026.10.08.757537). It has not been peer reviewed. The author-approved v10 manuscript and separate supporting-information PDFs remain available in `site/papers/`. The original v7 files remain available at their original URLs. It is not presented as a peer-reviewed journal publication. The source code release and scientific claims have separate reproducibility limitations documented in that repository.

The supplied Xeroth logo and manuscript remain author-owned material. Djuma history is paraphrased from [Djuma's own account](https://www.djuma.com/djumacam). Third-party data and models retain their own terms. Existing legacy pipeline files are retained; they do not form part of the deployed website.

## Website information and data presentation

The home page introduces OpenXeroth; `/naturecam/` explains Djuma Cam and the AI being built on it, and links to the official Djuma YouTube channel for the live stream.

The default bird view groups detections by species within a selected SAST calendar day, at 85% confidence or above. Daily cards, counts and hourly charts use the same records. The server follows up to twelve 1,000-record pages, deduplicates IDs and marks a capped result as partial; it never labels a partial sample as a complete day. Non-species sound categories remain available separately and do not inflate bird diversity. Full call histories and provider archive summaries remain accessible. The daily endpoint is cached for five minutes; the client checks for updates every minute.

`data/sabi-sand-birds.json` records the dated regional checklist, explicit spelling/name mappings and separately sourced range conflicts. Each species suggestion is placed in one of three groups. A suggestion that fails BirdNET-Cloud's eBird range check for the call, or is listed as a range conflict, is shown as a likely misidentification in a collapsed group below the others. A checklist species is shown first. Anything else that passes the eBird range check is shown separately as not on the 2019 checklist. A checklist match is not confirmation of an identification; an unmatched name is not proof of geographic absence. Taxonomic differences remain explicit. The [geographic review](docs/djuma-range-review.md) records how the check was tested. The highlight of the day is the checklist species, with audio and a verified photograph, recorded on the fewest station days since 26 August 2026, with ties broken by the highest model score; the page states why it was chosen. The Worker's cron (every 30 minutes) adds each completed SAST day to the `species-days-v1` record in the `LIVE_STATE` KV binding. If that record is unavailable the highlight falls back to the highest model score and says so.

Wikimedia Commons image metadata supplies each photograph's author, licence and licence link. Verified credits are cached for seven days. Images whose credits cannot be verified are withheld, while their Commons source link remains available. Neither checklist warnings nor image-credit lookups change the underlying recording archive or the research datasets.

Software DOI: https://doi.org/10.5281/zenodo.23167850. Evidence DOI: https://doi.org/10.5281/zenodo.23174588. The 6 October 2026 v10 PDFs use CC BY-NC 4.0; software remains Apache-2.0.
