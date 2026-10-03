import checklist from './data/sabi-sand-birds.json' with {type:'json'};
const names=new Set(checklist.names);
export function rangeAssessment(species={}) {
 const name=String(species.scientific_name||'').toLowerCase().trim(), base={source:checklist.source,reviewed:checklist.reviewed};
 if(name&&!/^[a-z]+ [a-z-]+(?: [a-z-]+)?$/.test(name))return {...base,status:'sound_label',label:'Other sound — not a bird species',note:'This is a model sound category, not a species identification.'};
 if(checklist.out_of_range[name]) return {...base,status:'outside_range',label:'Likely misidentification — outside known range',note:checklist.out_of_range[name].range,source:checklist.out_of_range[name].source};
 if(checklist.related[name])return {...base,status:'taxonomy_review',label:'Taxonomy needs review',note:checklist.related[name].note};
 if(names.has(name)||names.has(checklist.aliases[name]))return {...base,status:'listed',label:'On the Sabi Sand checklist',note:'Regional presence is plausible; it does not confirm this detection.'};
 return {...base,status:'not_listed',label:'Not on the regional checklist — review needed',note:'Not matched to the December 2019 Sabi Sand checklist. Absence from this dated list is not proof that a bird cannot occur here; check the sound, taxonomy and range before accepting this identification.'};
}
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
 for(const d of rows)d.range_review=rangeAssessment(d.species);
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
 const grouped=new Map(),counts=Array(24).fill(0),seen=new Set();
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
  if(rangeAssessment(d.species).status!=='sound_label')counts[(new Date(d.timestamp).getUTCHours()+2)%24]++;
 }
 const allGroups=[...grouped.values()].map(g=>({...g,avg_confidence:g.confidence_sum/g.count,range_review:rangeAssessment(g.species)})).sort((a,b)=>b.count-a.count||a.species.common_name.localeCompare(b.species.common_name));
 const groups=allGroups.filter(g=>g.range_review.status!=='sound_label'),other_sounds=allGroups.filter(g=>g.range_review.status==='sound_label');
 return {date,groups,other_sounds,counts,total_detections:groups.reduce((n,g)=>n+g.count,0),total_species:groups.length,review_species:groups.filter(g=>g.range_review.status!=='listed'||g.range_flagged).length};
}
