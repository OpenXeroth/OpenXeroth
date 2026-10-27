from pathlib import Path
from html import escape

ROOT = Path(__file__).resolve().parents[1] / 'site'
repo = 'https://github.com/OpenXeroth/acoustic-re-id-research'
preprint = 'https://www.biorxiv.org/content/10.64898/2026.10.08.757537v1'
preprint_doi = 'https://doi.org/10.64898/2026.10.08.757537'

def page(path, title, description, body, active='', script=False, extra_script=''):
    links = [('/naturecam/', 'NatureCam'), ('/djuma-birds/', 'Djuma birds'), ('/research/', 'Research'), ('https://base.xeroth.ai/', 'Base stations')]
    nav = ''.join(f'<a href="{u}"'+(' aria-current="page"' if n == active else '')+f'>{n}</a>' for u,n in links)
    content = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(title)} · OpenXeroth</title><meta name="description" content="{escape(description)}"><meta name="theme-color" content="#738878"><link rel="icon" href="/assets/xeroth-green.png"><link rel="canonical" href="https://open.xeroth.ai{path}"><meta property="og:title" content="{escape(title)} · OpenXeroth"><meta property="og:description" content="{escape(description)}"><link rel="stylesheet" href="/assets/site.css"></head><body><a class="skip" href="#main">Skip to content</a><header class="top"><div class="wrap"><div class="nav"><a class="brand" href="/" aria-label="OpenXeroth home"><img src="/assets/xeroth-green.png" alt="" width="40" height="40">Open<span>Xeroth</span></a><nav aria-label="Main navigation">{nav}<a class="nav-github" href="https://github.com/OpenXeroth">GitHub ↗</a></nav></div></div></header><main id="main" class="wrap">{body}</main><footer><div class="wrap footer-row"><div><span>OpenXeroth is part of <a href="https://xeroth.ai">Xeroth</a>.</span></div><div class="footer-links"><a href="https://www.youtube.com/@djumacam">Djuma Cam on YouTube ↗</a><a href="https://github.com/OpenXeroth">GitHub ↗</a><a href="https://www.djuma.com/djumacam">The Djuma story ↗</a></div></div></footer>{'<script type="module" src="/assets/birds.js"></script>' if script else ''}{extra_script}</body></html>'''
    p = ROOT / path.strip('/') / 'index.html';p.parent.mkdir(parents=True, exist_ok=True);p.write_text(content)

scene='''<div class="scene" role="img" aria-label="Illustration of the landscape around a waterhole, with an observation marker"><div class="scene-label">DJUMA · SOUTH AFRICA</div><svg viewBox="0 0 500 400" fill="none" aria-hidden="true"><circle cx="330" cy="88" r="48" fill="#f8f9ee"/><path d="M-20 250Q95 185 230 244T540 225V440H-20Z" fill="#ced9c1"/><path d="M-20 289Q105 225 255 280T530 265V420H-20Z" fill="#aabd9f"/><path d="M-20 335Q110 283 250 340T530 305V420H-20Z" fill="#8da389"/><path d="M200 352Q285 324 401 350Q470 369 504 398H93Q118 374 200 352" fill="#dce5d6"/><path d="M278 363L420 363M207 376H452M256 387H484" stroke="#aabd9f"/><path d="M84 317L95 154M94 212L66 181M93 205L122 174" stroke="#657f63" stroke-width="5"/><path d="M40 166Q49 133 83 142Q96 112 119 136Q153 125 163 159Q176 177 122 175Q79 188 40 166" fill="#6f8869"/><path d="M390 300L398 235M398 260L413 245" stroke="#718c6d" stroke-width="3"/><path d="M365 239Q376 217 393 225Q410 205 432 230Q444 247 412 246Z" fill="#7c9675"/><path d="M176 127l8-4 8 4M202 117l8-4 8 4" stroke="#728877" stroke-width="2"/></svg><div class="scene-cross"></div><div class="scene-chip"><strong>People · cameras · sound</strong>At Gowrie Dam</div><div class="scene-note">DJUMA CAM · SINCE 1998</div></div>'''
wave='<div class="wave" aria-hidden="true">'+''.join(f'<i style="height:{h}px"></i>' for h in [8,15,12,24,48,28,15,55,80,45,100,68,35,16,28,56,85,46,28,14,40,69,44,24,10,18,31,20,10])+'</div>'
page('/', 'Open tools for understanding wildlife', 'OpenXeroth shares Xeroth’s open tools and research using optics, acoustics and AI. Explore NatureCam, Djuma birds and acoustic re-identification research.', f'''
<section class="hero"><div><div class="eyebrow">OpenXeroth</div><h1>Open tools for<br><em>understanding wildlife.</em></h1><p class="lead">We use cameras, sound and AI to study wildlife. We share the tools and evidence behind the work.</p><p>OpenXeroth is part of <a href="https://xeroth.ai">Xeroth</a>, whose mission is to become the trusted operating layer for autonomous AI in high-stakes environments, from protecting wildlife to protecting nations. This is where our open conservation projects and research can be explored, used and questioned.</p></div><figure class="sensing-visual"><svg viewBox="0 0 480 330" role="img" aria-labelledby="sensing-title"><title id="sensing-title">The camera and the microphone each produce their own open record</title><rect x="1" y="1" width="478" height="328" rx="24" fill="#eaf0e7"/><g fill="none" stroke="#365f47" stroke-width="3"><rect x="54" y="56" width="104" height="72" rx="12"/><circle cx="106" cy="92" r="23"/><circle cx="106" cy="92" r="10"/><path d="M168 92H280M55 231h20l8-26 13 52 12-75 13 94 12-70 12 47 12-22h28M185 231H280"/><rect x="282" y="56" width="143" height="72" rx="12"/><path d="M304 80h95m-95 24h65"/><rect x="282" y="195" width="143" height="72" rx="12"/><path d="M304 219h95m-95 24h82"/></g><g fill="#20382d" font-family="system-ui,sans-serif" font-size="18"><text x="65" y="160">Optics</text><text x="62" y="310">Acoustics</text><text x="296" y="160">Camera record</text><text x="304" y="300">Sound record</text></g></svg></figure></section>
<section class="section intro-projects"><div class="eyebrow">Explore OpenXeroth</div><h2>Choose where to begin.</h2><div class="grid3"><article class="card"><span class="number">01 / NatureCam</span><h3>Our live African waterhole fuelling AI research</h3><p>Watch Djuma Cam, and see the AI we are building on it: a world model, individual re-identification and a co-pilot for its volunteer camera operators.</p><a class="link" href="/naturecam/">Explore NatureCam ↗</a></article><article class="card"><span class="number">02 / Djuma birds</span><h3>Automated identification of birds at the waterhole</h3><p>Browse daily species suggestions and listen to calls analysed with BirdNET-Cloud.</p><a class="link" href="/djuma-birds/">Explore Djuma birds ↗</a></article><article class="card"><span class="number">03 / Research</span><h3>AI research papers and articles</h3><p>Our papers and articles on what AI learns from biological recordings, and how to test it, with sources, code and data.</p><a class="link" href="/research/">Explore the research ↗</a></article></div><p class="open-hardware">Also open: <a href="https://base.xeroth.ai/">GNSS base stations</a>. Build one from a Raspberry Pi, add it to the Xeroth network, or download RINEX from the network for centimetre positioning.</p></section>''')
page('/naturecam/', 'NatureCam: wildlife observation at Djuma', 'Djuma Cam, a live pan-tilt-zoom camera at an African waterhole, and the AI Xeroth is building on it: a world model, individual re-identification and a co-pilot for volunteer camera operators.', f'''
<section class="hero"><div><div class="breadcrumbs"><a href="/">OpenXeroth</a> / NatureCam</div><div class="eyebrow">The NatureCam project</div><h1>A live camera at an African waterhole.<br><em>And the AI we are building on it.</em></h1><p class="lead">Djuma Cam is a pan-tilt-zoom (PTZ) camera overlooking Gowrie Dam in the Sabi Sand, South Africa. Volunteer camera operators, known as zoomies, steer it remotely from around the world: they turn it, tilt it and zoom in to follow the animals.</p><p>On this camera we are developing three things: a world model, which is an AI understanding of the scene the camera watches; individual re-identification, which recognises animals that return; and an AI co-pilot that helps the operators find and follow animals.</p><p>Open to everyone: the live stream on Djuma’s YouTube channel, and every bird call the camera’s microphone records, published with its recording, spectrogram and AI species suggestion on <a href="/djuma-birds/">Djuma birds</a>. You can run your own models on the live stream or on those recordings and compare their answers with the published suggestions.</p><div class="actions"><a class="button" href="https://www.youtube.com/@djumacam/live">Watch Djuma Cam live on YouTube <span>↗</span></a><a class="button secondary" href="/djuma-birds/">Listen to the bird calls</a></div></div>{scene}</section>
<section class="section" id="naturecam"><div class="eyebrow">01 / The NatureCam project</div><div class="grid3"><article class="card"><span class="number">01</span><h3>Observe</h3><p>Djuma’s camera began sharing images in 1998. Generations of viewers and volunteer camera operators have built a shared record of life at the same waterhole, a history that gives each new observation context.</p><a class="link" href="https://www.youtube.com/@djumacam">Watch Djuma Cam ↗</a></article><article class="card"><span class="number">02</span><h3>Recognise</h3><p>Hosana is the re-identification pipeline we’re building. Its goal is to connect repeat sightings of the same animal over time, starting at Djuma. Individual recognition is still being researched and validated.</p><span class="tag">In development</span></article><article class="card"><span class="number">03</span><h3>Control</h3><p>Zoomies are the people who remotely pan, tilt and zoom nature cameras. We’re developing an AI zoomie to help them notice activity, follow animals and choose useful views, with human judgement at the centre.</p><span class="tag">In development</span></article></div></section>
''', 'NatureCam')
page('/research/acoustic-re-id-research/','Can AI be used to identify individual animals accurately?', 'Plain-English guide to our acoustic re-identification research, with the full paper, source code, dataset and model links.', f'''
<div class="subhero"><div class="breadcrumbs"><a href="/">OpenXeroth</a> / <a href="/research/">Research</a> / Acoustic re-identification</div><div class="eyebrow">Acoustic re-identification research</div><h1>Can AI be used to identify <br><em>individual animals accurately?</em></h1><p class="lead">Previous studies suggest AI can recognise individual animals from their calls. We found that recording conditions and the way recordings are divided for testing can inflate that apparent accuracy. Our study examines how much the background, microphone and recording session contribute to the result.</p><div class="actions"><a class="button" href="{preprint}">Read the bioRxiv preprint <span>↗</span></a><a class="button secondary" href="{repo}">Explore the code <span>↗</span></a></div></div><div class="stats"><div class="stat"><span class="big-number">9</span><small>species · eight birds and one bat</small></div><div class="stat"><span class="big-number">13</span><small>datasets evaluated</small></div><div class="stat"><span class="big-number">36</span><small>neural-network models, compared with two simple reference methods</small></div></div>
<section class="section prose"><div class="eyebrow">The question</div><h2>What could individual recognition make possible?</h2><p>Following the same animal over time could help researchers study survival, movement, social relationships and responses to environmental change. Recognising a familiar caller could extend that record beyond what a camera can see, with less need to catch, tag or disturb animals.</p><p>Earlier studies have approached this with machine learning (ML): computers learn patterns from labelled recordings and use them to suggest which animal made a new call. The promise is considerable, but a useful system must recognise an animal in a different session, with a different background, and when unfamiliar animals arrive.</p><p>Imagine recognising a friend on the phone. Their voice should remain familiar when they call from a busy street or a different handset. In animal recordings, a microphone or background can instead become an accidental name tag. We tested how much that affects the evidence for individual recognition.</p></section>
<figure class="research-visual"><div><span class="number">What goes into a recording?</span><div class="visual-components"><span>Animal’s voice</span><b>+</b><span>Background</span><b>+</b><span>Microphone &amp; session</span></div><div class="visual-arrow" aria-hidden="true">↓</div><div class="visual-result">An AI identity prediction</div></div><figcaption>A good test asks which part of the recording supports the prediction. This is a conceptual diagram, not measured data.</figcaption></figure>
<section class="section"><div class="eyebrow">What we tested</div><h2>Four ways to check<br>if the computer got it right.</h2><article class="finding"><span class="num">01</span><div><h3>Change the recording session.</h3><p>The unit being divided matters. Training on some of Monday’s calls and testing on other calls from Monday can share background and recording conditions. Training on Monday and testing on Tuesday asks whether the model recognises the bird in a new session. We compared these designs using the same recordings and the same training and test counts for each bird. Across 18 comparisons, three species and ten fixed random splits, randomly dividing individual clips raised measured accuracy by 20–51 percentage points. That higher score does not show improved recognition on a new day. Whole sessions can themselves be assigned randomly while keeping their clips separate.</p></div></article><article class="finding"><span class="num">02</span><div><h3>Take the animal’s voice away.</h3><p>Could the system still predict the assigned identity from background alone? For chiffchaff recordings within a year, ordinary accuracy was 73.7% from backgrounds versus 76.5% from calls. Giving every bird equal weight produces balanced accuracies of 71.4% and 79.1%. The background result remains substantial, although the two scores are not equivalent. A high score can carry information about the recording context as well as the animal.</p></div></article><article class="finding"><span class="num">03</span><div><h3>Hold the microphone and recording constant.</h3><p>Where several animals shared a recording, we could ask whether the model distinguished them beyond that shared context. Of three datasets with this stronger test, only one zebra finch dataset gave clear evidence of identity information beyond the recording. Timing is also a limitation: the two BirdPark datasets contain no same-bird pairs from different recordings within seven days. They cannot provide a same-day check that separates recording changes from changes over time.</p></div></article><article class="finding"><span class="num">04</span><div><h3>Let strangers arrive.</h3><p>In nature, a caller may be an animal the system has never met. We set an acceptance threshold using separate unfamiliar animals, targeting no more than one in ten strangers. We then tested two ways of making a match: comparing a call with each familiar bird’s average sound profile, or with its closest individual training call. On wild datasets separated by session, the highest median proportion of familiar calls accepted and correctly named rose from 37% with average profiles to 46% with nearest-call matching. These maxima came from little owl with four enrolled birds, across 36 networks and two scoring methods. They describe the tested methods, not limits on what future models can achieve. The median is over different assignments of birds to test roles, and measures calls rather than individual animals. Actual stranger acceptance could exceed the target; using more calibration strangers did not consistently solve this.</p></div></article><p class="fine">These are specific benchmark results, not forecasts of Djuma performance. See the paper for uncertainty intervals, dataset qualifications and the complete comparisons.</p></section>
<section class="section prose"><div class="eyebrow">The study at a glance</div><h2>Four checks, with their limits.</h2><figure><a href="/papers/graphical-abstract-v10.pdf"><img src="/assets/graphical-abstract-v10.png" alt="Study summary: random clip splits raise measured accuracy; background predicts assigned identity; timing limits shared-recording tests; open-set results depend on scoring and calibration." width="3200" height="1800" style="display:block;width:100%;height:auto"></a><figcaption><a href="/papers/graphical-abstract-v10.pdf">Download the illustrated summary · PDF</a>. Read <a href="{repo}/tree/main/docs/review-sensitivity">the complete follow-up results and limitations</a>.</figcaption></figure><p>One further limitation remains: the added-background experiment used a single fixed assignment of background donors. Testing several donor assignments would be a separate robustness experiment.</p></section>
<section class="section"><div class="feature"><div><div class="eyebrow">Why it matters</div><h2>Better tests.<br>More trustworthy tools.</h2></div><div><p>We found no neural network that won on every dataset. Choosing a different internal layer also changed the rankings. The useful lesson is to test the entire recognition process against the conditions it will actually face.</p><p>For NatureCam and Hosana, that means treating individual identity as a claim to validate. Today’s BirdNET-Cloud detections at Djuma are species suggestions. They do not establish how many individual birds are present.</p><div class="notice">The study uses existing published datasets. It does not use Djuma Cam audio as a benchmark or demonstrate a working individual bird recogniser at Djuma.</div></div></div></section>
<section class="section"><div class="eyebrow">Read the paper. Inspect the methods.</div><h2>The work is here to explore.</h2><div class="paper-box"><span class="tag">bioRxiv preprint · Version 1</span><h3 style="margin-top:20px">Recording context and data splitting can inflate acoustic re-identification accuracy across species and studies</h3><p>Graham Wallington &amp; Jackie Lighten · Xeroth AI Limited</p><div class="actions"><a class="button" href="{preprint}">Read on bioRxiv ↗</a><a class="button secondary" href="/papers/acoustic-re-id-v10.pdf">Author manuscript · PDF ↓</a><a class="button secondary" href="/papers/supporting-information-v10.pdf">Supporting information · PDF ↓</a></div><p style="margin-top:22px">This paper is now available as a bioRxiv preprint. It has not been peer reviewed. Preprint DOI: <a href="{preprint_doi}">10.64898/2026.10.08.757537</a>. The verification audit reproduced the reported error rates and checked all 224 original result files against their archive copies. The review follow-up adds 194 independently archived outputs covering matched splits, alternative scoring, calibration size and timing controls. Software and a separate evidence bundle are archived with permanent DOI links. Recordings, model weights and real identity annotations remain subject to their original publishers’ terms.</p><p>The paper and original evidence documentation use <a href="https://creativecommons.org/licenses/by-nc/4.0/">CC BY-NC 4.0</a>: share and adapt with credit for non-commercial use. The software retains its Apache 2.0 licence. Xeroth AI Limited funded the research; both authors are company officers and shareholders.</p></div><div class="grid3" style="margin-top:22px"><article class="card"><h3>Source code</h3><p>Analysis methods, model adapters, controls and software tests.</p><a class="link" href="{repo}">Open the public repository ↗</a><br><a class="link" href="https://doi.org/10.5281/zenodo.23167850">Archived software · DOI ↗</a></article><article class="card"><h3>Datasets &amp; models</h3><p>Original publishers, model registries and their separate licence terms.</p><a class="link" href="{repo}/blob/main/docs/datasets.md">Find the datasets ↗</a><br><a class="link" href="{repo}/blob/main/docs/models.md">Find the models ↗</a></article><article class="card"><h3>Reproducibility</h3><p>How to verify the software, source provenance and what a numerical rerun still requires.</p><a class="link" href="{repo}/blob/main/docs/reproduction.md">Read the reproduction guide ↗</a><br><a class="link" href="https://doi.org/10.5281/zenodo.23174588">Evidence bundle · DOI ↗</a></article></div></section>''','Research')
for path,mode,title,lead in [('/djuma-birds/','feed','Who’s calling at Djuma?','Listen to the birdsong around Gowrie Dam. Explore the latest species suggestions from Djuma Cam, in partnership with BirdNET-Cloud.'),('/djuma-birds/species/','species','A closer listen.','Explore this species’ detection history at Djuma Cam.'),('/djuma-birds/call/','call','One call. A moment in the bush.','An individual detection from Djuma Cam’s soundscape.')]:
    page(path,title,'Public BirdNET-Cloud species detections from Djuma Cam. Latest calls, species histories and individual detection records.',f'''<div id="birds-app" data-mode="{mode}"><div class="subhero"><div class="breadcrumbs"><a href="/">OpenXeroth</a> / <a href="/djuma-birds/">Djuma birds</a></div><div class="eyebrow">Djuma Cam × BirdNET-Cloud</div><h1 id="bird-title">{title}</h1><p class="lead" id="bird-lead">{lead}</p><div class="status" id="feed-status" role="status">Connecting to the detection feed…</div></div><div class="notice">These are automated species suggestions, not confirmed sightings or individual identities. Confidence is a model score, not the probability that a suggestion is correct. Times below are South African Standard Time (UTC+2).</div><div id="bird-content" aria-busy="true"><p class="loading">Loading the soundscape…</p></div></div><section class="section"><div class="prose"><h2>About these detections.</h2><p>One bird may produce many detections, and more than one bird may be present in a clip. Detection totals are not counts of animals. The feed refreshes every 60 seconds; upstream processing and interruptions can delay new records.</p><p><a href="https://birdnetcloud.com/s/djuma-cam-b1610b">Visit Djuma on BirdNET-Cloud ↗</a> · <a href="https://www.youtube.com/@djumacam">Watch Djuma Cam ↗</a></p><a class="link" href="/research/acoustic-re-id-research/">Why recognising individuals is harder ↗</a></div></section>''','Djuma birds',True)

import re
import json
from datetime import date
ARTICLES = Path(__file__).resolve().parents[1] / 'research' / 'articles'


def long_date(iso):
    d = date.fromisoformat(iso)
    return f'{d.day} {d:%B %Y}'


def read_article(folder):
    """research/articles/<slug>/article.md: front matter, then '# Title', then the lead paragraph and the body."""
    text = (folder / 'article.md').read_text()
    head, body = text.split('\n---\n', 1)
    meta = dict(line.split(': ', 1) for line in head.removeprefix('---\n').splitlines())
    body = body.lstrip('\n')
    assert body.startswith(f"# {meta['title']}\n"), folder
    lead, rest = body.split('\n', 1)[1].lstrip('\n').split('\n\n', 1)
    chart = folder / 'chart.json'
    return {**meta, 'kind': 'Article', 'path': f'/research/{folder.name}/', 'lead': lead, 'body': rest,
            'chart': json.loads(chart.read_text()) if chart.exists() else None}


# Papers have hand-written guide pages above. Articles are every folder in research/articles/.
PAPERS = [
    {'path': '/research/acoustic-re-id-research/', 'kind': 'Paper · bioRxiv preprint, not peer reviewed', 'date': '2026-10-09',
     'title': 'Can AI be used to identify individual animals accurately?',
     'summary': 'How recording conditions and the way recordings are divided for testing inflate the accuracy of AI that recognises individual animals from their calls, tested across nine species and 13 datasets. Plain-English guide, paper, code and evidence.'},
]
RESEARCH = sorted(PAPERS + [read_article(f) for f in sorted(ARTICLES.iterdir()) if f.is_dir()], key=lambda r: r['date'], reverse=True)


def chart_svg(chart):
    rows = [(r['label'], r['source'], r['shared_context'], r['kept_apart'], r['chance']) for r in chart['rows']]
    w, left, right, row_h, top = 520, 40, 492, 78, 10
    x = lambda v: left + (right - left) * v / 100
    bottom = top + len(rows) * row_h
    gaps = [leaky - proper for _, _, leaky, proper, _ in rows]
    title = f'Same model, two tests: the leaky test scores {round(min(gaps))} to {round(max(gaps))} points higher'
    out = [f'<svg viewBox="0 0 {w} {bottom + 34}" role="img" aria-labelledby="effect-title" class="effect-chart"><title id="effect-title">{escape(title)}</title>']
    for t in (0, 25, 50, 75, 100):
        out.append(f'<line x1="{x(t):.1f}" y1="{top + 26}" x2="{x(t):.1f}" y2="{bottom - 4}" class="c-grid"/><text x="{x(t):.1f}" y="{bottom + 18}" text-anchor="middle" class="c-note">{t}%</text>')
    for i, (label, src, leaky, proper, chance) in enumerate(rows):
        y = top + i * row_h
        cy = y + 52
        out.append(f'<text x="0" y="{y + 14}" class="c-label">{escape(label)}<tspan class="c-src"> · {escape(src)}</tspan></text>')
        out.append(f'<line x1="{x(proper):.1f}" y1="{cy}" x2="{x(leaky):.1f}" y2="{cy}" class="c-bar"/>')
        if chance is not None:
            out.append(f'<line x1="{x(chance):.1f}" y1="{cy - 11}" x2="{x(chance):.1f}" y2="{cy + 11}" class="c-chance"/>')
        out.append(f'<circle cx="{x(proper):.1f}" cy="{cy}" r="7" class="c-proper"><title>{escape(label)}: {proper:g}% with sessions, sites or blocks kept apart</title></circle>')
        out.append(f'<circle cx="{x(leaky):.1f}" cy="{cy}" r="7" class="c-leaky"><title>{escape(label)}: {leaky:g}% with shared context</title></circle>')
        out.append(f'<text x="{x(proper) - 12:.1f}" y="{cy + 5}" text-anchor="end" class="c-value">{proper:g}</text>')
        if leaky < 88:
            out.append(f'<text x="{x(leaky) + 12:.1f}" y="{cy + 5}" class="c-value">{leaky:g}</text>')
        else:
            out.append(f'<text x="{x(leaky):.1f}" y="{cy - 14}" text-anchor="middle" class="c-value">{leaky:g}</text>')
    out.append('</svg>')
    legend = ('<p class="chart-legend"><span><i class="k-leaky"></i>test shares a session, site or time block with training</span>'
              '<span><i class="k-proper"></i>kept apart</span><span><i class="k-chance"></i>chance</span></p>')
    return f'<figure class="chart-figure"><p class="chart-title">{escape(title)}</p>{legend}{"".join(out)}<figcaption>{escape(chart['caption'])}</figcaption></figure>'


LINK = re.compile(r'\[([^\]]+)\]\((<[^>]+>|[^)\s]+)\)|\*\*(.+?)\*\*')


def inline(t):
    out, pos = [], 0
    for m in LINK.finditer(t):
        out.append(escape(t[pos:m.start()], quote=False))
        if m.group(1) is not None:
            out.append(f'<a href="{escape(m.group(2).strip("<>"))}">{inline(m.group(1))}</a>')
        else:
            out.append(f'<strong>{inline(m.group(3))}</strong>')
        pos = m.end()
    out.append(escape(t[pos:], quote=False))
    return ''.join(out)


def slug(t):
    return re.sub(r'[^a-z0-9]+', '-', t.lower()).strip('-')


def markdown_html(md, chart=None):
    html, refs = [], False
    for block in [b.strip('\n') for b in re.split(r'\n\s*\n', md) if b.strip()]:
        lines = block.split('\n')
        if block == '<!-- chart -->':
            html.append(chart_svg(chart))
        elif block.startswith('## '):
            text = block[3:].strip()
            refs = text == 'Sources'
            html.append(f'<h2 id="{slug(text)}">{inline(text)}</h2>')
        elif all(l.startswith('|') for l in lines):
            rows = [[c.strip() for c in l.strip().strip('|').split('|')] for l in lines if not re.fullmatch(r'\|[\s\-:|]+\|', l.strip())]
            head = ''.join(f'<th scope="col">{inline(c)}</th>' for c in rows[0])
            body = ''.join('<tr>' + ''.join(f'<td>{inline(c)}</td>' for c in r) + '</tr>' for r in rows[1:])
            html.append(f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>')
        elif all(l.startswith('- ') for l in lines):
            items = ''.join(f'<li>{inline(l[2:])}</li>' for l in lines)
            html.append(('<ul class="refs">' if refs else '<ul>') + items + '</ul>')
        else:
            html.append(f'<p>{inline(" ".join(l.strip() for l in lines))}</p>')
    return ''.join(html)


def research_card(r):
    return (f'<article class="paper-box research-item"><p class="research-meta"><span class="tag">{escape(r["kind"])}</span>'
            f'<time datetime="{r["date"]}">{long_date(r["date"])}</time></p><h3><a href="{r["path"]}">{escape(r["title"])}</a></h3>'
            f'<p>{escape(r["summary"])}</p><a class="link" href="{r["path"]}">Read it ↗</a></article>')


page('/research/', 'Research', 'Xeroth’s AI research papers and articles on what machine learning learns from biological recordings, with sources, code and data.',
     '<div class="subhero"><div class="breadcrumbs"><a href="/">OpenXeroth</a> / Research</div><div class="eyebrow">OpenXeroth research</div>'
     '<h1>Research papers<br><em>and articles.</em></h1><p class="lead">Xeroth’s research on artificial intelligence and biological data. '
     'Papers link to their code, data and evidence. Articles link to every source they cite.</p></div>'
     f'<section class="section research-list">{"".join(research_card(r) for r in RESEARCH)}</section>', 'Research')

for r in RESEARCH:
    if r['kind'] != 'Article':
        continue
    page(r['path'], r['title'], r['summary'],
         f'<div class="subhero"><div class="breadcrumbs"><a href="/">OpenXeroth</a> / <a href="/research/">Research</a> / Article</div>'
         f'<div class="eyebrow">Article</div><h1>{escape(r["title"])}</h1><p class="lead">{inline(r["lead"])}</p>'
         f'<p class="fine">By {escape(r["author"])} · <time datetime="{r["date"]}">{long_date(r["date"])}</time></p></div>'
         f'<article class="article">{markdown_html(r["body"], r["chart"])}</article>', 'Research')
(ROOT/'404.html').write_text('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Page not found · OpenXeroth</title><link rel="stylesheet" href="/assets/site.css"><main class="wrap subhero"><h1>Off the trail.</h1><p>We could not find that page.</p><a class="button" href="/">Return to OpenXeroth</a></main></html>')
(ROOT/'robots.txt').write_text('User-agent: *\nAllow: /\nDisallow: /api/\nSitemap: https://open.xeroth.ai/sitemap.xml\n')
(ROOT/'sitemap.xml').write_text('<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'+''.join(f'<url><loc>https://open.xeroth.ai{p}</loc></url>' for p in ['/','/naturecam/','/djuma-birds/','/research/']+[r['path'] for r in RESEARCH])+'</urlset>')
