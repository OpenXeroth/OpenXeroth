import checklist from './data/sabi-sand-birds.json' with {type:'json'};
const names=new Set(checklist.names);
const share=n=>n<0.001?'under 0.1%':`${Math.round(n*1000)/10}%`;
const ebirdPage=species=>species?.ebird_code?`https://ebird.org/species/${encodeURIComponent(species.ebird_code)}`:'https://ebird.org/explore';
// evidence.flagged is BirdNET-Cloud's eBird range verdict for the call (true when it
// failed); evidence.occurrence is the share of regional eBird checklists reporting it.
export function rangeAssessment(species={},evidence={}) {
 const name=String(species.scientific_name||'').toLowerCase().trim(), base={source:checklist.source,reviewed:checklist.reviewed};
 const occ=typeof evidence.occurrence==='number'?evidence.occurrence:null, ebird=ebirdPage(species);
 if(name&&!/^[a-z]+ [a-z-]+(?: [a-z-]+)?$/.test(name))return {...base,status:'sound_label',tier:'sound',label:'Other sound, not a bird species',note:'This is a model sound category, not a species identification.'};
 if(checklist.out_of_range[name]) return {...base,status:'outside_range',tier:'misidentification',label:'Likely misidentification: outside its known range',note:checklist.out_of_range[name].range,source:checklist.out_of_range[name].source};
 const listed=names.has(name)||names.has(checklist.aliases[name]);
 if(evidence.flagged===true&&listed)return {...base,status:'listed_flagged',tier:'checklist',label:'The location check flags this call',note:'The species is on the Sabi Sand checklist, but this call failed the eBird range check. Listen before accepting it.',source:ebird};
 if(evidence.flagged===true)return {...base,status:'range_conflict',tier:'misidentification',label:'Likely misidentification: outside its known range',note:`Regional eBird records almost never include this species${occ===null?'':` (${share(occ)} of checklists)`}, so the model’s suggestion is probably wrong. Listen before accepting it.`,source:ebird};
 if(checklist.related[name])return {...base,status:'taxonomy_review',tier:'checklist',label:'Taxonomy needs review',note:checklist.related[name].note};
 if(listed)return {...base,status:'listed',tier:'checklist',label:'On the Sabi Sand checklist',note:'Regional presence is plausible; it does not confirm this detection.'};
 if(evidence.flagged===false)return {...base,status:'regional',tier:'regional',label:'Not on the 2019 Sabi Sand checklist',note:`Regional eBird records include this species${occ===null?'':` (${share(occ)} of checklists)`}, so it may occur here, but the December 2019 Sabi Sand checklist does not list it. Listen before accepting it.`,source:ebird};
 return {...base,status:'not_listed',tier:'regional',label:'Not on the 2019 Sabi Sand checklist',note:'Not matched to the December 2019 Sabi Sand checklist, and no eBird range check is attached to this summary. Absence from a dated list is not proof that a bird cannot occur here; listen before accepting it.'};
}
export const detectionEvidence=d=>({flagged:typeof d?.passed_range_filter==='boolean'||typeof d?.location_filtered==='boolean'?(d.passed_range_filter===false||d.location_filtered===true):undefined,occurrence:d?.range_occurrence??d?.occurrence});
const plain=value=>String(value||'').replace(/<[^>]*>/g,' ').replace(/&#(\d+);/g,(_,n)=>String.fromCodePoint(Number(n))).replace(/&(?:amp|quot|lt|gt|nbsp);/g,x=>({'&amp;':'&','&quot;':'"','&lt;':'<','&gt;':'>','&nbsp;':' '})[x]).replace(/\s+/g,' ').trim().slice(0,700);
const normal=title=>title.replace(/_/g,' ');
export function commonsTitle(d){try{return decodeURIComponent(new URL(d.photo.page_url).pathname.split('/wiki/')[1]);}catch{return null;}}
export function creditFromMetadata(info) {
 const m=info?.extmetadata||{},author=plain(m.Artist?.value),license=plain(m.LicenseShortName?.value);
 let license_url='';try{const u=new URL(m.LicenseUrl?.value);if(u.protocol==='https:'&&['creativecommons.org','www.gnu.org'].includes(u.hostname))license_url=u.href;}catch{}
 return author&&license?{photographer:author,license,license_url,verified:true}:null;
}
const creditPending=new Map();
export async function enrichBirds(items,cache,ctx) {
 const rows=Array.isArray(items)?items:[items],credits=new Map();
 for(const d of rows)d.range_review=rangeAssessment(d.species,detectionEvidence(d));
 const titles=[...new Set(rows.map(commonsTitle).filter(Boolean))].sort();
 // Cache batches, not every row: a repeated species costs one lookup and a
 // 200-row page stays comfortably within the worker's subrequest budget.
 for(let i=0;i<titles.length;i+=50){
  const batch=titles.slice(i,i+50),batchKey=batch.join('|');
  const digest=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(batchKey));
  const hash=[...new Uint8Array(digest)].map(n=>n.toString(16).padStart(2,'0')).join('');
  const key=new Request('https://open.xeroth.ai/commons-credit-v2/'+hash);
  const hit=await cache.match?.(key);
  if(hit){for(const [title,credit] of Object.entries(await hit.json()))credits.set(title,credit);continue;}
  if(!creditPending.has(batchKey))creditPending.set(batchKey,(async()=>{
   const q=new URLSearchParams({action:'query',format:'json',prop:'imageinfo',iiprop:'extmetadata',titles:batchKey});
   const r=await fetch('https://commons.wikimedia.org/w/api.php?'+q,{headers:{'User-Agent':'OpenXeroth/1.0 (https://open.xeroth.ai; Wikimedia image attribution)'},redirect:'manual',signal:AbortSignal.timeout(6000)});
   if(!r.ok)throw new Error('credit-http-'+r.status);
   const found={};
   for(const p of Object.values((await r.json()).query?.pages||{})){const credit=creditFromMetadata(p.imageinfo?.[0]);if(credit)found[normal(p.title)]=credit;}
   return found;
  })().finally(()=>creditPending.delete(batchKey)));
  try{
   const found=await creditPending.get(batchKey);
   for(const [title,credit] of Object.entries(found))credits.set(title,credit);
   if(Object.keys(found).length)ctx.waitUntil?.(cache.put?.(key,Response.json(found,{headers:{'Cache-Control':'public, max-age=604800'}}))||Promise.resolve());
  }catch(e){console.warn('Wikimedia attribution unavailable',e.name,e.message.slice(0,100));}
 }
 for(const d of rows){const title=commonsTitle(d),credit=credits.get(normal(title||''));if(credit)d.photo={...d.photo,...credit};else if(d.photo_url){delete d.photo_url;if(d.photo)d.photo.verified=false;}}
 return items;
}
export function summarizeDay(rows,date,minimum) {
 const grouped=new Map(),counts=Array(24).fill(0),hourSpecies=Array.from({length:24},()=>new Set()),seen=new Set();
 for(const d of rows){
  if(seen.has(d.id))continue;seen.add(d.id);
  if(new Date(new Date(d.timestamp).getTime()+7200000).toISOString().slice(0,10)!==date||d.confidence<minimum)continue;
  const key=d.species.scientific_name||d.species.common_name;
  let g=grouped.get(key);
  if(!g){g={species:d.species,representative:d,count:0,first:d.timestamp,last:d.timestamp,max_confidence:0,confidence_sum:0,range_flagged:0};grouped.set(key,g);}
  g.count++;g.confidence_sum+=d.confidence;g.max_confidence=Math.max(g.max_confidence,d.confidence);
  if(d.timestamp<g.first)g.first=d.timestamp;if(d.timestamp>g.last)g.last=d.timestamp;
  if(d.confidence>g.representative.confidence || (!g.representative.audio_available&&d.audio_available))g.representative=d;
  if(d.passed_range_filter===false||d.location_filtered)g.range_flagged++;
  if(rangeAssessment(d.species).status!=='sound_label'){const h=(new Date(d.timestamp).getUTCHours()+2)%24;counts[h]++;hourSpecies[h].add(key);}
 }
 const allGroups=[...grouped.values()].map(g=>({...g,avg_confidence:g.confidence_sum/g.count,range_review:rangeAssessment(g.species,{flagged:g.range_flagged>0,occurrence:g.representative.range_occurrence??g.representative.occurrence})})).sort((a,b)=>b.count-a.count||a.species.common_name.localeCompare(b.species.common_name));
 const groups=allGroups.filter(g=>g.range_review.tier!=='sound'),other_sounds=allGroups.filter(g=>g.range_review.tier==='sound');
 return {date,groups,other_sounds,counts,species_counts:hourSpecies.map(s=>s.size),total_detections:groups.reduce((n,g)=>n+g.count,0),total_species:groups.length,review_species:groups.filter(g=>g.range_review.tier==='misidentification').length};
}
// The species of every complete station day, kept so the daily highlight can favour
// the checklist species heard on the fewest days since the station started.
export const HISTORY_KEY='species-days-v1';
export const HISTORY_START='2026-08-26';
export function daySpecies(rows){
 const found=new Set();
 for(const d of rows){const name=String(d.species?.scientific_name||'').toLowerCase().trim();if(name&&rangeAssessment(d.species).tier!=='sound')found.add(name);}
 return [...found].sort();
}
export function addDay(history,date,species){
 const h=history&&history.through?{...history,days:{...history.days}}:{since:HISTORY_START,through:null,recorded_days:0,days:{}};
 if(h.through&&date<=h.through)return h;
 if(species.length){h.recorded_days++;for(const name of species)h.days[name]=(h.days[name]||0)+1;}
 h.through=date;return h;
}
export function pickHighlight(groups,history){
 const candidates=groups.filter(g=>g.range_review.status==='listed'&&g.representative.audio_available&&g.representative.photo?.verified);
 if(!candidates.length)return null;
 const known=history&&history.through&&history.recorded_days>0;
 const days=g=>known?(history.days[String(g.species.scientific_name||'').toLowerCase()]||0):0;
 const best=[...candidates].sort((a,b)=>days(a)-days(b)||b.max_confidence-a.max_confidence||a.species.common_name.localeCompare(b.species.common_name))[0];
 return known?{key:best.species.scientific_name,reason:'rarest',days:days(best),recorded_days:history.recorded_days,since:history.since,through:history.through}:{key:best.species.scientific_name,reason:'score'};
}
