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

`PUBLIC_BIRD_MEDIA` defaults to `false` pending confirmation that Djuma's BirdNET-Cloud audio and spectrograms may be republished publicly. When authorised, set it to `true` and redeploy. Media is then fetched server-side from the exact Djuma bucket/object namespace, with no redirects; callers cannot specify a destination URL. Unavailable/expired media does not remove the detection record. No bucket-wide public permissions are required.

Species pages use the provider's common-name filter. Call membership is checked against the Djuma media namespace or a signed call link issued from this station’s detection feed. `PUBLIC_CALL_KEY` is a random HMAC signing secret stored only in Cloudflare, never in Git. These public call links allow historical metadata to remain accessible even when its recording is unavailable. Rotating the key invalidates older historical call links; re-open the call from its species feed to obtain a current link. Media remains separately permission-gated.

## Research

[Public research repository](https://github.com/OpenXeroth/acoustic-re-id-research). The PDF in `site/papers/` is the edited v7 author manuscript, exported 2 October 2026. It is not presented as a peer-reviewed journal publication. The source code release and scientific claims have separate reproducibility limitations documented in that repository.

The supplied Xeroth logo and manuscript remain author-owned material. Djuma history is paraphrased from [Djuma's own account](https://www.djuma.com/djumacam). Third-party data and models retain their own terms. Existing legacy pipeline files are retained; they do not form part of the deployed website.
