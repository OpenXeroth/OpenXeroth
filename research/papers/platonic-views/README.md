# Platonic views research guide

Prepared URL: `https://open.xeroth.ai/research/platonic-views/`.

This is an unpublished website candidate. Build with `npm run build`. The hand-written guide uses the same shared page shell and research components as the bioacoustic paper. Edit `guide.html`; the build writes `site/research/platonic-views/index.html`. Its additional assets are `site/assets/platonic-views.css` and `platonic-views.js`.

The guide is deliberately absent from `PAPERS`, the research index and the sitemap. It has `noindex, nofollow`. An unlisted URL is not access control; anyone who has the URL could read it after deployment. This change does not deploy the website.

## Evidence

Copy and measured results follow manuscript revision 1.3, frozen in `OpenXeroth/platonic-views-artifacts` at `paper-v1.3`. The original benchmark neighbour result is in section 4.2; SEN12TP replication is in 4.3; vegetation-index readouts are in 4.4. The figure values give each geographic block equal weight. Conceptual diagrams are labelled separately from measured results.

The original hypothesis and theory papers are linked beside their explanations. The experiment does not establish the theorem's assumptions, convergence with scale, or forecasting/planning ability. Same-observation crop overlap is a material confound retained in the guide.

## Publication handoff

When publication is authorised:

1. Confirm the author-approved manuscript revision and update any values, pinned links and revision date if it changes.
2. Add the actual arXiv abstract URL when assigned. Replace the pending arXiv text with a link; do not use the theory paper's arXiv identifier for this manuscript.
3. Confirm the curated GitHub repository and both Hugging Face repositories are public and the pinned links work anonymously. Code/results are `paper-v1.3`; unchanged models/evaluation inputs are `paper-v1.2`.
4. Publish the existing Zenodo record for reserved DOI `10.5281/zenodo.24102082` and verify its public landing page and files. This is the artifact DOI, not an assigned arXiv DOI.
5. Update the preparation notice, manuscript badge and resource access/status text to match the released state. Replace the staged model/data descriptions where needed.
6. Remove the guide's robots metadata in `tools/build_site.py` when it should be indexed. Add its entry to `PAPERS` only when the research index should link to it; the sitemap follows that list automatically.
7. Build, check the diagrams and publication links on desktop and mobile, and follow the repository's normal website deployment instructions.

Do not link the private working research repository, review correspondence or cloud archives. The guide links only the curated publication materials and original sources.
