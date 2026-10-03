const root = document.querySelector('#birds-app');
const mode = root.dataset.mode;
const content = document.querySelector('#bird-content');
const status = document.querySelector('#feed-status');
const params = new URLSearchParams(location.search);
const speciesName = params.get('name') || '';
const id = params.get('id') || '';
let currentPage = 1, perPage = 50, minimum = '0.85', busy = false, rerun = false;
let allSpecies = [], search = '', period = 'all', station = null, histogram = null;
let records = new Map(), playingId = null;
let selectedDate = new Date(Date.now()+7200000).toISOString().slice(0,10);

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c]));
const number = n => Number(n || 0).toLocaleString('en-GB');
const pct = n => typeof n === 'number' ? `${Math.round(n * 1000) / 10}%` : 'Not supplied';
const speciesHref = name => `/djuma-birds/species/?name=${encodeURIComponent(name)}`;
const callHref = d => `/djuma-birds/call/?id=${encodeURIComponent(d.id)}${d.access ? '&access=' + encodeURIComponent(d.access) : ''}`;
const mediaHref = (d, kind) => `/api/birds/call/${encodeURIComponent(d.id)}/${kind}${d.access?'?access='+encodeURIComponent(d.access):''}`;
const wikiHref = species => species?.scientific_name ? `https://en.wikipedia.org/wiki/${encodeURIComponent(species.scientific_name.replace(/ /g, '_'))}` : '';
const time = (value, full = false) => {
  const date = new Date(value);
  if (!value || Number.isNaN(date.getTime())) return 'Not supplied';
  return new Intl.DateTimeFormat('en-GB', {timeZone:'Africa/Johannesburg', ...(full ? {dateStyle:'long', timeStyle:'medium'} : {day:'2-digit', month:'short', hour:'2-digit', minute:'2-digit', second:'2-digit'})}).format(date);
};
const age = value => {
  const m = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 60000));
  if (!Number.isFinite(m)) return 'Time unavailable';
  return m < 1 ? 'less than a minute ago' : m < 60 ? `${m} minutes ago` : m < 1440 ? `${Math.floor(m/60)}h ${m%60}m ago` : `${Math.floor(m/1440)} days ago`;
};
const relative = timestamp => `<span data-relative="${esc(timestamp)}">${age(timestamp)}</span>`;
const scientific = species => wikiHref(species) ? `<a class="call-sci" href="${wikiHref(species)}" target="_blank" rel="noopener noreferrer" title="Read about this species on Wikipedia">${esc(species.scientific_name)} ↗</a>` : '';

async function get(path) {
  const r = await fetch('/api/birds/' + path, {credentials:'omit', signal:AbortSignal.timeout(20000)});
  const p = await r.json();
  if (!r.ok) throw new Error(p.error || 'The feed is unavailable.');
  return p;
}
function photo(d, large = false) {
  const source=d.photo?.page_url;
  if (!d.photo_url || !source || !d.photo?.verified) return `<div class="photo-placeholder${large ? ' large' : ''}">Photograph unavailable${source?`<br><a href="${esc(source)}" target="_blank" rel="noopener noreferrer">View Wikimedia source ↗</a>`:''}</div>`;
  return `<figure class="bird-photo${large ? ' large' : ''}"><a href="${esc(source)}" target="_blank" rel="noopener noreferrer"><img class="species-photo" src="${esc(d.photo_url)}" alt="${esc(d.species?.common_name || 'Bird species')} — reference photograph" loading="lazy" width="330" height="240"></a><figcaption>${esc(d.photo.photographer)} · <a href="${esc(d.photo.license_url||source)}" target="_blank" rel="noopener noreferrer">${esc(d.photo.license)}</a> · <a href="${esc(source)}" target="_blank" rel="noopener noreferrer">Source ↗</a></figcaption></figure>`;
}
function rangeNote(d) {
  const r=d.range_review;
  const upstream=d.location_filtered||d.passed_range_filter===false||d.range_flagged>0;
  if(!r)return '<p class="range-flag">Geographic check unavailable — review this identification.</p>';
  if(r.status==='listed'&&!upstream)return '';
  return `<aside class="range-review ${r.status==='outside_range'?'range-conflict':''}"><strong>${esc(r.status==='listed'?'Location model flags this identification':r.label)}</strong><p>${esc(r.status==='listed'?'The species is on the regional checklist, but this call was flagged by the location model. Listen and check before accepting it.':r.note)}</p><a href="${esc(r.source)}" target="_blank" rel="noopener noreferrer">Check the range source ↗</a></aside>`;
}
function facts(rows) {
  return `<dl class="details">${rows.filter(([,v]) => v !== undefined && v !== null).map(([k,v]) => `<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl>`;
}
function feedStatus(timestamp, updated) {
  const delayed = !timestamp || Date.now() - new Date(timestamp).getTime() > 15 * 60000;
  status.classList.toggle('delayed', delayed);
  status.textContent = `Feed checked ${time(updated)} SAST · ${timestamp ? 'Latest recording ' + age(timestamp) : 'No recording timestamp available'}${delayed ? ' · Recordings are delayed' : ''}`;
}
function mediaButtons(d) {
  return `<div class="call-actions">${d.audio_available ? `<button class="small-button" data-action="play" data-id="${esc(d.id)}" aria-label="Play ${esc(d.species.common_name)} call recorded ${time(d.timestamp)}">${playingId === d.id && !player.paused ? 'Pause' : '▶ Listen'}</button>` : '<span class="fine">Audio unavailable</span>'}<a class="link" href="${callHref(d)}">View call ↗</a></div>`;
}
function row(d) {
  const occurrence = d.range_occurrence ?? d.occurrence;

  return `<article class="detection-row">${photo(d)}<div class="detection-info"><a class="call-name" href="${speciesHref(d.species.common_name)}">${esc(d.species.common_name || 'Unknown species')}</a>${scientific(d.species)}<p class="call-time"><time datetime="${esc(d.timestamp)}">${time(d.timestamp)} SAST</time> · ${relative(d.timestamp)}</p><div class="call-measures"><span><strong>${pct(d.confidence)}</strong> model score</span>${typeof occurrence === 'number' ? `<span title="Share of regional eBird checklists reporting this species, not confidence in this identification">${pct(occurrence)} of eBird lists</span>` : ''}${d.species.ebird_code ? `<span>eBird: ${esc(d.species.ebird_code)}</span>` : ''}</div>${rangeNote(d)}</div>${d.spectrogram_available ? `<button class="spectrogram-preview" data-action="spectrogram" data-id="${esc(d.id)}" aria-label="Enlarge spectrogram of ${esc(d.species.common_name)} call"><img src="${mediaHref(d,'spectrogram')}" alt="Spectrogram — open full size" loading="lazy" width="180" height="70"><span>Enlarge spectrogram ⤢</span></button>` : '<span class="fine spectrogram-missing">Spectrogram unavailable</span>'}${mediaButtons(d)}</article>`;
}
function pager(meta, count) {
  const total = typeof meta.total === 'number' ? meta.total : null;
  const pageCount = total === null ? null : Math.max(1, Math.ceil(total/perPage));
  const more = pageCount === null ? count === perPage : currentPage < pageCount;
  return `<div class="pager"><button id="previous" ${currentPage===1?'disabled':''}>← Newer</button><span>Page ${number(currentPage)}${pageCount ? ' of '+number(pageCount) : ''}${total !== null ? ' · '+number(total)+' recordings' : ''}</span><button id="next" ${!more?'disabled':''}>Older →</button></div>`;
}
function floors() {
  const gate = station?.filter_config?.min_confidence || 0;
  return [['0', gate ? `All available (station floor ${pct(gate)})` : 'All available detections'], ...[0.7,0.85,0.95].filter(v=>v>gate).map(v=>[String(v),`${pct(v)} and above`])];
}
function toolbar() {
  return `<div class="toolbar"><label>Model confidence<select id="minimum">${floors().map(([v,label])=>`<option value="${v}" ${minimum===v?'selected':''}>${esc(label)}</option>`).join('')}</select></label><label>Recordings per page<select id="per-page">${[25,50,100,200].map(v=>`<option value="${v}" ${perPage===v?'selected':''}>${v}</option>`).join('')}</select></label><button class="button secondary" id="refresh">Refresh feed ↻</button></div>`;
}
function chart(input, label = 'Detections by hour') {
  const values = Array.from({length:24}, (_,i)=>Math.max(0, Number(input?.[i])||0));
  const max = Math.max(...values,1);
  return `<div class="chart" role="img" aria-label="${esc(label)} in South African Standard Time. ${values.map((v,h)=>`${h}:00: ${v}`).join('; ')}">${values.map((v,h)=>`<div class="chart-bar" style="height:${Math.max(2,Math.round(128*v/max))}px" title="${h}:00 · ${number(v)} detections">${h%3===0?`<span>${String(h).padStart(2,'0')}</span>`:''}</div>`).join('')}</div>`;
}
function speciesGrid() {
  const shown = allSpecies.filter(d=>`${d.species.common_name} ${d.species.scientific_name}`.toLowerCase().includes(search.toLowerCase()));
  document.querySelector('#species-count').textContent = `${shown.length} species suggestions`;
  document.querySelector('#species-grid').innerHTML = shown.length ? shown.map(d=>`<article class="species-card">${photo(d,true)}<div class="species-card-body"><a class="call-name" href="${speciesHref(d.species.common_name)}">${esc(d.species.common_name)} ↗</a>${scientific(d.species)}${rangeNote(d)}<div class="species-count">${number(d.detection_count)} detection${d.detection_count===1?'':'s'}</div><p class="fine">${pct(d.avg_confidence)} average score · ${pct(d.max_confidence)} best<br>First heard ${time(d.first_heard)}<br>Last heard ${time(d.last_heard)} · ${relative(d.last_heard)}</p><a class="link" href="${speciesHref(d.species.common_name)}">Explore recordings ↗</a></div></article>`).join('') : '<p class="fine">No species match this search or period.</p>';
}
function dailyTable(stats) {
  if (!Array.isArray(stats?.daily_activity) || !stats.daily_activity.length) return '';
  const rows = [...stats.daily_activity].reverse(), max = Math.max(1,...rows.map(d=>Number(d.detections)||0));
  return `<section class="section"><div class="eyebrow">Recent days</div><h2>A daily record of the soundscape.</h2><div class="daily-table"><table><caption class="sr-only">Daily detections and species reported by BirdNET-Cloud</caption><thead><tr><th scope="col">Date · SAST</th><th scope="col">Detections</th><th scope="col">Species</th><th scope="col"><span class="sr-only">Relative activity</span></th></tr></thead><tbody>${rows.map(d=>`<tr><th scope="row">${esc(d.date)}</th><td>${number(d.detections)}</td><td>${number(d.species)}</td><td><div class="daily-bar" style="width:${Math.max(0,Number(d.detections)||0)/max*100}%"></div></td></tr>`).join('')}</tbody></table></div><p class="fine">Historical provider totals, using the station’s own filters rather than the selected day’s confidence setting. Counts are detections, not individual birds; zero does not establish that birds were absent.</p></section>`;
}
function stationDetails() {
  if (!station) return '';
  const f = station.filter_config || {}, cap = station.media_retention?.effective_cap_per_species_per_day;
  return `<section class="section"><div class="eyebrow">About this feed</div><h2>The listening station.</h2>${facts([['Station',station.name],['Location',station.location_label],['Station ID',station.slug],['Detects',station.kinds?.join(', ')],['Audio source',station.source_type?.toUpperCase()],['Timezone',station.timezone],['Position',typeof station.latitude==='number'?`${station.latitude}, ${station.longitude}`:null],['Listening since',time(station.first_detection,true)+' SAST'],['Latest station detection',time(station.last_detection,true)+' SAST'],['Current confidence floor',pct(f.min_confidence)],['eBird range model',f.range_model_enabled?`On · flags below ${pct(f.range_occurrence_threshold)}`:'Off'],['Media retention policy',station.media_retention?.configured],['Daily clip limit',cap===0?'No per-species daily cap':typeof cap==='number'?`${cap} per species per day`:null]])}<p class="fine" style="margin-top:24px">Identifications come from BirdNET (Cornell Lab of Ornithology and Chemnitz University of Technology) via BirdNET-Cloud. Detections are made from sound alone: the bird need not be on camera. Photographs are species illustrations, not pictures of the individual calling. Each photograph links to its Wikimedia Commons file page for the author and licence.</p><a class="link" href="https://birdnetcloud.com/s/djuma-cam-b1610b" target="_blank" rel="noopener noreferrer">This station on BirdNET-Cloud ↗</a></section>`;
}
function speciesOverview(summary) {
  if (!summary) return '';
  return `<section class="species-overview">${photo(summary,true)}<div><div class="eyebrow">Species record</div><h2>${esc(summary.species.common_name)}</h2>${scientific(summary.species)}${rangeNote(summary)}${facts([['Detections in provider summary',number(summary.detection_count)],['Average model score',pct(summary.avg_confidence)],['Highest model score',pct(summary.max_confidence)],['First heard',time(summary.first_heard,true)+' SAST'],['Last heard',time(summary.last_heard,true)+' SAST'],['eBird code',summary.species.ebird_code]])}</div></section>`;
}
function speciesActivity() {
  if (!histogram?.sample_size) return '';
  const busiest = histogram.counts.indexOf(Math.max(...histogram.counts));
  return `<section class="section"><div class="eyebrow">When this species calls</div><h2>The rhythm of ${esc(speciesName)}.</h2>${chart(histogram.counts,`${speciesName} detections by hour`)}<p class="chart-caption">Hour of day in SAST (UTC+2), from the most recent ${number(histogram.sample_size)} detections (${time(histogram.earliest)} to ${time(histogram.latest)}). Busiest hour in this sample: ${String(busiest).padStart(2,'0')}:00. This sample is not the complete species history.</p></section>`;
}
function guide() {
  return `<p class="fine">The model score describes the match between the sound and a species. “Of eBird lists” describes how often regional birdwatchers report that species; it is not confidence in this identification, and can be low for nocturnal or secretive birds. ${station?.filter_config?.min_confidence ? `The station reports a ${pct(station.filter_config.min_confidence)} confidence floor. ` : ''}Only detections supplied by the station can appear here. Range-flagged detections remain visible.</p>`;
}
function bindPage() {
  document.querySelector('#previous').onclick = ()=>{currentPage--;load();};
  document.querySelector('#next').onclick = ()=>{currentPage++;load();};
  document.querySelector('#minimum').onchange = e=>{minimum=e.target.value;currentPage=1;load();};
  document.querySelector('#per-page').onchange = e=>{perPage=Number(e.target.value);currentPage=1;load();};
  document.querySelector('#refresh').onclick = ()=>load();
}

// One shared player lets visitors listen directly from a feed or species page.
const playerBar = document.createElement('aside');
playerBar.className = 'bird-player hidden';
playerBar.setAttribute('aria-label','Bird call player');
playerBar.innerHTML = '<div><strong id="player-label"></strong><span id="player-message" class="fine" role="status"></span></div><audio controls preload="none"></audio><button class="small-button" id="close-player" aria-label="Close player">Close ×</button>';
root.append(playerBar);
const player = playerBar.querySelector('audio');
const playerLabel = playerBar.querySelector('#player-label');
const playerMessage = playerBar.querySelector('#player-message');
function updatePlayButtons() {
  for (const button of content.querySelectorAll('[data-action="play"]')) button.textContent = button.dataset.id===playingId && !player.paused ? 'Pause' : '▶ Listen';
}
for (const event of ['play','pause','ended']) player.addEventListener(event,updatePlayButtons);
player.addEventListener('error',()=>{playerMessage.textContent='This recording is unavailable or has expired. Its detection record remains available.';});
playerBar.querySelector('#close-player').onclick = ()=>{player.pause();player.removeAttribute('src');player.load();playingId=null;playerBar.classList.add('hidden');updatePlayButtons();};
async function play(d) {
  if (playingId===d.id && !player.paused) {player.pause();return;}
  player.pause();playingId=d.id;player.src=mediaHref(d,'audio');playerLabel.textContent=`${d.species.common_name} · ${time(d.timestamp)} SAST`;playerMessage.textContent='';playerBar.classList.remove('hidden');
  try {await player.play();} catch {playerMessage.textContent='Playback could not start. Try the player’s play button, or another call.';}
}
const dialog = document.createElement('dialog');
dialog.className = 'spectrogram-dialog';
dialog.setAttribute('aria-label','Full-size call spectrogram');
root.append(dialog);
function showSpectrogram(d) {
  dialog.innerHTML = `<div class="dialog-heading"><div><h3>${esc(d.species.common_name)}</h3><p class="fine">${time(d.timestamp,true)} SAST</p></div><button class="small-button" id="close-spectrogram" aria-label="Close spectrogram">Close ×</button></div><img class="spectrogram" src="${mediaHref(d,'spectrogram')}" alt="Spectrogram of the ${esc(d.species.common_name)} call"><p class="fine">Frequency against time. Brighter marks show stronger sound. This is the audio used for the identification, not an image of the bird.</p><a class="link" href="${callHref(d)}">Open this call’s page ↗</a>`;
  dialog.querySelector('#close-spectrogram').onclick = ()=>dialog.close();
  dialog.querySelector('img').onerror = e=>{e.target.hidden=true;const note=document.createElement('p');note.textContent='This spectrogram is unavailable or has expired.';dialog.append(note);};
  dialog.showModal();
}
dialog.addEventListener('click',e=>{if(e.target===dialog)dialog.close();});
content.addEventListener('click',e=>{
  const button=e.target.closest('button[data-action]');
  if (!button) return;
  const d=records.get(button.dataset.id);if(!d)return;
  if(button.dataset.action==='play')play(d);
  if(button.dataset.action==='spectrogram')showSpectrogram(d);
});
content.addEventListener('error',e=>{
  if(e.target.matches('img.species-photo')) {e.target.hidden=true;const figure=e.target.closest('figure');if(!figure.querySelector('.image-error')){const note=document.createElement('span');note.className='image-error fine';note.textContent='Photograph unavailable — view its Wikimedia source below.';figure.prepend(note);}}
  else if(e.target.matches('.spectrogram-preview img')){e.target.hidden=true;e.target.nextElementSibling.textContent='Spectrogram unavailable';}
  else if(e.target.matches('audio,.spectrogram')){if(e.target.matches('img'))e.target.hidden=true;const note=document.querySelector('#media-error');if(note)note.textContent='This recording is temporarily unavailable or has expired. Its detection record remains available.';}
},true);

function dailyCard(g) {
 const d=g.representative;
 return `<article class="daily-species-card">${photo(d,true)}<div class="daily-species-body"><a class="call-name" href="${speciesHref(g.species.common_name)}">${esc(g.species.common_name)} ↗</a>${scientific(g.species)}${rangeNote(g)}<p><strong>${number(g.count)} call${g.count===1?'':'s'}</strong> · ${pct(g.max_confidence)} highest model score</p><p class="fine">First ${time(g.first)} · Last ${time(g.last)} SAST</p>${mediaButtons(d)}<a class="link" href="${speciesHref(g.species.common_name)}">Explore this species’ call history ↗</a>${d.spectrogram_available?`<button class="small-button" data-action="spectrogram" data-id="${esc(d.id)}">View this call’s spectrogram ⤢</button>`:''}</div></article>`;
}
async function loadDailyFeed() {
 const q=new URLSearchParams({date:selectedDate,min_confidence:minimum});
 const [daily,stationResult,latest]=await Promise.all([get('daily?'+q),get('station').catch(()=>null),get('detections?per_page=25').catch(()=>null)]);
 station=stationResult?.data||station;
 const d=daily.data;
 records=new Map([...d.groups,...d.other_sounds].map(g=>[g.representative.id,g.representative]));
 feedStatus(latest?.data[0]?.timestamp,daily.meta.fetched_at);
 const latestDate=latest?.data[0]?.timestamp?new Date(new Date(latest.data[0].timestamp).getTime()+7200000).toISOString().slice(0,10):null;
 const featured=d.groups.filter(g=>g.range_review.status==='listed'&&!g.range_flagged&&g.representative.audio_available&&g.representative.photo?.verified).sort((a,b)=>a.count-b.count||b.max_confidence-a.max_confidence)[0];
 content.innerHTML=`<section class="section daily-intro"><div class="eyebrow">One species card per day</div><h2>The day in birdsong.</h2><div class="toolbar"><label>Date · SAST<input type="date" id="day" min="2026-08-01" max="${new Date(Date.now()+7200000).toISOString().slice(0,10)}" value="${selectedDate}"></label><label>Model confidence<select id="minimum">${floors().map(([v,label])=>`<option value="${v}" ${minimum===v?'selected':''}>${esc(label)}</option>`).join('')}</select></label><button class="button secondary" id="refresh">Refresh ↻</button></div><p>Repeated calls are grouped under the model’s species suggestion. One card does not mean one individual bird. The counts and cards below use the same date and confidence setting.</p>${daily.meta.complete?'':'<p class="notice">This is a partial daily sample: the request reached its safety limit. Counts describe the loaded records, not the complete day.</p>'}<div class="stats"><div class="stat"><span class="big-number">${number(d.total_species)}</span><small>species suggestions in this view</small></div><div class="stat"><span class="big-number">${number(d.total_detections)}</span><small>calls in this view</small></div><div class="stat"><span class="big-number">${number(d.review_species)}</span><small>species suggestions needing geographic or taxonomy review</small></div></div></section>${featured?`<section class="daily-highlight"><div>${photo(featured.representative,true)}</div><div><div class="eyebrow">Listen closely · ${selectedDate}</div><h2>${esc(featured.species.common_name)}</h2><p>This is one of the less frequently detected species suggestions in the selected day’s records: ${number(featured.count)} call${featured.count===1?'':'s'}. It appears on the regional checklist, but this recording still needs a human ear.</p><p class="fine">A listening highlight, not a confirmed rare sighting. The photograph is a reference image of the species.</p>${mediaButtons(featured.representative)}</div></section>`:''}<section class="section" id="detections"><h2>Species suggested on ${selectedDate}</h2><p class="fine">Geographic warnings stay visible. <a href="https://github.com/OpenXeroth/OpenXeroth/blob/main/docs/djuma-range-review.md">Read the full checklist review ↗</a></p><div class="daily-species-grid feed">${d.groups.length?d.groups.map(dailyCard).join(''):`<p class="loading">No calls match this date and confidence setting.${latestDate&&latestDate!==selectedDate?` <button class="small-button" id="latest-day" data-date="${latestDate}">Open the latest recorded day (${latestDate})</button>`:''}</p>`}</div>${d.other_sounds.length?`<details class="archive-panel"><summary>Other sounds (${d.other_sounds.reduce((n,g)=>n+g.count,0)} detections)</summary><p>Non-species sound labels are kept here and excluded from the bird totals above.</p>${d.other_sounds.map(dailyCard).join('')}</details>`:''}</section>${d.total_detections?`<section class="section"><div class="eyebrow">Activity in this view</div><h2>When the calls were recorded.</h2>${chart(d.counts)}<p class="chart-caption">${number(d.total_detections)} calls on ${selectedDate}, using the confidence setting above. Hours are SAST (UTC+2).</p></section>`:''}<details class="archive-panel"><summary>Explore the wider species catalogue and station history</summary><p>These archive summaries come from BirdNET-Cloud and use its station filters. They are separate from the selected-day counts above.</p><div id="archive-content"><button class="button secondary" id="load-archive">Load archive summaries</button></div></details>${stationDetails()}`;
 document.querySelector('#day').onchange=e=>{if(e.target.value){selectedDate=e.target.value;load();}};
 document.querySelector('#minimum').onchange=e=>{minimum=e.target.value;load();};
 document.querySelector('#refresh').onclick=()=>load();
 const dayLink=document.querySelector('#latest-day');if(dayLink)dayLink.onclick=()=>{selectedDate=dayLink.dataset.date;load();};
 document.querySelector('#load-archive').onclick=loadArchive;
}
async function loadArchive() {
 const target=document.querySelector('#archive-content');target.setAttribute('aria-busy','true');
 try{
  const [species,stats]=await Promise.all([get('species?period='+period),get('stats').catch(()=>null)]);allSpecies=species.data;
  target.innerHTML=`<h2>Species catalogue</h2><div class="toolbar"><label>Search species<input id="species-search" type="search" placeholder="Try nightjar or hoopoe" value="${esc(search)}"></label><label>Archive period<select id="period">${[['all','All time'],['today','Today'],['7d','Last 7 days'],['30d','Last 30 days']].map(([v,l])=>`<option value="${v}" ${period===v?'selected':''}>${l}</option>`).join('')}</select></label><span class="fine" id="species-count"></span></div><div id="species-grid" class="species-grid"></div>${dailyTable(stats?.data)}`;
  speciesGrid();document.querySelector('#species-search').oninput=e=>{search=e.target.value;speciesGrid();};document.querySelector('#period').onchange=e=>{period=e.target.value;loadArchive();};
 }catch{target.innerHTML='<p>Archive summaries are temporarily unavailable. Close and reopen the page to try again.</p>';}
 finally{target.setAttribute('aria-busy','false');}
}

async function load(initial = false) {
  if(busy){rerun=true;return;}
  busy=true;content.setAttribute('aria-busy','true');
  try {
    if(mode==='feed') { await loadDailyFeed(); } else if(mode==='call') {
      if(!/^[a-f0-9-]{36}$/i.test(id))throw new Error('Open a call from the Djuma birds feed to see its record.');
      const p=await get('call/'+encodeURIComponent(id)+'?access='+encodeURIComponent(params.get('access')||'')),d=p.data;
      d.access=params.get('access')||d.access;
      records=new Map([[d.id,d]]);
      document.querySelector('#bird-title').textContent=d.species.common_name;
      document.querySelector('#bird-lead').textContent=d.species.scientific_name;
      document.title=`${d.species.common_name} call · OpenXeroth`;
      status.textContent=`Recorded ${time(d.timestamp,true)} SAST · ${age(d.timestamp)}`;
      const occurrence=d.range_occurrence??d.occurrence;
      content.innerHTML=`<section class="section"><div class="actions" style="margin:0 0 28px"><a class="button secondary" href="${speciesHref(d.species.common_name)}">All ${esc(d.species.common_name)} detections ↗</a><button class="button secondary" id="copy">Copy this call’s link</button></div><div class="call-overview">${photo(d,true)}<div><h2>One call, in detail.</h2>${scientific(d.species)}${rangeNote(d)}<p>Recorded ${time(d.timestamp,true)} SAST · ${relative(d.timestamp)}</p><p><strong>${pct(d.confidence)}</strong> model score${typeof occurrence==='number'?` · ${pct(occurrence)} of regional eBird lists`:''}</p>${d.audio_available?`<audio controls preload="none" src="${mediaHref(d,'audio')}" aria-label="Play ${esc(d.species.common_name)} call">Your browser does not support audio playback.</audio>`:`<p class="notice">${d.media_public?'Audio for this call is unavailable.':'Audio is not published on this page.'} The detection record remains available.</p>`}</div></div>${d.spectrogram_available?`<section class="call-spectrogram"><h3>The sound, visualised</h3><img class="spectrogram" src="${mediaHref(d,'spectrogram')}" alt="Spectrogram of this ${esc(d.species.common_name)} call"><p class="fine">Frequency against time. Brighter marks show stronger sound. This is the audio used for the identification, not an image of the bird.</p></section>`:'<p class="notice">The spectrogram is unavailable. The detection record remains available below.</p>'}${facts([['Species suggestion',d.species.common_name],['Scientific name',d.species.scientific_name],['Recorded',time(d.timestamp,true)+' SAST'],['Model confidence',pct(d.confidence)],['Share of regional eBird lists',typeof occurrence==='number'?pct(occurrence):null],['Provider range check',d.passed_range_filter===false?'Flagged as unexpected here':d.passed_range_filter===true?'Passed':'Not supplied'],['Location filter flag',typeof d.location_filtered==='boolean'?(d.location_filtered?'Flagged':'Not flagged'):null],['eBird code',d.species.ebird_code],['Kind',d.kind],['Processed by station',time(d.created_at,true)+' SAST'],['Detection ID',d.id]])}<div id="media-error" class="fine" role="status"></div><p class="fine" style="margin-top:24px">The share of eBird lists is about regional birdwatching reports, not whether this identification is correct. It can run low for nocturnal and secretive birds. The reference photograph shows the species, not this individual; its source link provides the photographer and licence.</p></section>`;
      document.querySelector('#copy').onclick=async e=>{try{await navigator.clipboard.writeText(location.href);e.target.textContent='Link copied';}catch{e.target.textContent='Copy the address from your browser';}};
    } else {
      if(mode==='species'&&!speciesName)throw new Error('Choose a species from the Djuma birds page.');
      const q=new URLSearchParams({page:String(currentPage),per_page:String(perPage),min_confidence:minimum});if(mode==='species')q.set('species',speciesName);
      const [calls,stats,species,latest,stationResult,sample]=await Promise.all([
        get('detections?'+q),mode==='feed'?get('stats').catch(()=>null):null,get('species?period='+(mode==='species'?'all':period)),
        currentPage>1||mode==='species'||minimum!=='0'?get('detections').catch(()=>null):null,
        get('station').catch(()=>null),mode==='species'&&!histogram?get('histogram?species='+encodeURIComponent(speciesName)).catch(()=>null):null
      ]);
      station=stationResult?.data||station;histogram=sample?.data||histogram;
      allSpecies=species.data;records=new Map(calls.data.map(d=>[d.id,d]));
      feedStatus((latest||calls).data[0]?.timestamp,calls.meta.fetched_at);
      const summary=mode==='species'?allSpecies.find(s=>s.species.common_name===speciesName):null;
      if(mode==='species'){
        document.querySelector('#bird-title').textContent=speciesName;document.title=`${speciesName} at Djuma · OpenXeroth`;
        document.querySelector('#bird-lead').textContent=summary?`${summary.species.scientific_name} · A history of this species’ calls at Djuma Cam.`:'Detections for this species suggestion at Djuma Cam.';
      }
      const hourly=stats?.data.hourly_activity, hourTotal=Array.isArray(hourly)?hourly.reduce((a,b)=>a+(Number(b)||0),0):0;
      content.innerHTML=`${stats?`<div class="stats bird-stats" style="margin-top:30px">${[[stats.data.species_today,'species suggestions today'],[stats.data.detections_today,'detections today'],[stats.data.total_species,'species suggestions to date'],[stats.data.total_detections,'detections to date']].map(([n,l])=>`<div class="stat"><span class="big-number">${number(n)}</span><small>${l}</small></div>`).join('')}</div><p class="fine">Summary counts use BirdNET-Cloud’s station filters. The feed below includes range-flagged detections, so totals may differ.</p>`:''}${mode==='species'?speciesOverview(summary)+speciesActivity():''}<section class="section" id="detections"><div class="eyebrow">${currentPage===1?'The latest recordings':'Earlier recordings'}</div><h2>${mode==='species'?'Calls through time.':'The soundscape, one call at a time.'}</h2>${toolbar()}${guide()}<div class="feed">${calls.data.length?calls.data.map(row).join(''):'<p class="loading">No detections match these filters.</p>'}</div>${pager(calls.meta,calls.data.length)}</section>${hourTotal?`<section class="section"><div class="eyebrow">The rhythm of the day</div><h2>When the soundscape stirs.</h2>${chart(hourly)}<p class="chart-caption">BirdNET-Cloud’s hourly summary: ${number(hourTotal)} detections, in SAST (UTC+2). Busiest hour reported: ${String(stats.data.busiest_hour).padStart(2,'0')}:00. The provider does not specify the summary’s time range. These are detections, not individual birds.</p></section>`:''}${mode==='feed'?`<section class="section"><div class="eyebrow">Explore the species</div><h2>Familiar voices. New discoveries.</h2><p class="fine">Open a species for its recordings, its scientific name for Wikipedia, or its photograph for the Wikimedia Commons source and licence.</p><div class="toolbar"><label>Search species<input id="species-search" type="search" placeholder="Try nightjar or hoopoe" value="${esc(search)}"></label><label>Detection period<select id="period">${[['all','All time'],['today','Today'],['7d','Last 7 days'],['30d','Last 30 days']].map(([v,l])=>`<option value="${v}" ${period===v?'selected':''}>${l}</option>`).join('')}</select></label><span class="fine" id="species-count"></span></div><div id="species-grid" class="species-grid"></div></section>${dailyTable(stats?.data)}${stationDetails()}`:''}`;
      bindPage();
      if(mode==='feed'){
        speciesGrid();document.querySelector('#species-search').oninput=e=>{search=e.target.value;speciesGrid();};document.querySelector('#period').onchange=e=>{period=e.target.value;load();};
      }
    }
  } catch(e) {
    status.textContent='The feed could not be updated. '+e.message;status.classList.add('delayed');
    if(initial||!content.querySelector('.feed')){
      content.innerHTML=`<div class="error" style="margin-top:30px" role="alert"><p>${esc(e.message)}</p><button class="button secondary" id="retry">Try again</button> <a href="/djuma-birds/">Back to Djuma birds</a></div>`;
      document.querySelector('#retry').onclick=()=>load(true);
    } else status.textContent+=' Showing the last successful update.';
  } finally {
    busy=false;content.setAttribute('aria-busy','false');if(rerun){rerun=false;load();}
  }
}
load(true);
const refreshTimer=setInterval(()=>{if(mode!=='call'&&!document.querySelector('.archive-panel[open]')&&currentPage===1&&!document.hidden&&!content.contains(document.activeElement)&&player.paused&&!dialog.open)load();},60000);
const ageTimer=setInterval(()=>{if(!document.hidden)for(const el of document.querySelectorAll('[data-relative]'))el.textContent=age(el.dataset.relative);},30000);
window.addEventListener('pagehide',()=>{clearInterval(refreshTimer);clearInterval(ageTimer);player.pause();});
