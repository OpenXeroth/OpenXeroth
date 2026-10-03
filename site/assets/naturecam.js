const button=document.querySelector('#load-stream'),cover=document.querySelector('#stream-cover'),status=document.querySelector('#stream-status');
let player,timer;
function fallback(message){clearTimeout(timer);player?.destroy();player=null;if(!document.querySelector('#djuma-player')){const slot=document.createElement('div');slot.id='djuma-player';document.querySelector('#live-frame').prepend(slot);}cover.hidden=false;button.disabled=false;button.textContent='Try the stream again ▶';document.querySelector('#stream-message').textContent=message;status.textContent='You can check the official channel or explore recorded bird calls below.';}
button.addEventListener('click',async()=>{
 button.disabled=true;status.textContent='Connecting to Djuma’s current YouTube broadcast…';
 try{
  const r=await fetch('/api/naturecam/live',{signal:AbortSignal.timeout(15000)}),data=await r.json();
  if(!r.ok||!data.available||!data.video_id)throw new Error('No broadcast could be confirmed');
  if(!window.YT?.Player)await new Promise((resolve,reject)=>{window.onYouTubeIframeAPIReady=resolve;const script=document.createElement('script');script.src='https://www.youtube.com/iframe_api';script.onerror=reject;document.head.append(script);setTimeout(()=>reject(new Error('Player unavailable')),12000);});
  cover.hidden=true;
  timer=setTimeout(()=>fallback('The stream could not start here. It may be offline, or your browser may have blocked the player.'),20000);
  player=new window.YT.Player('djuma-player',{host:'https://www.youtube-nocookie.com',videoId:data.video_id,playerVars:{autoplay:1,mute:1,playsinline:1,origin:location.origin},events:{onReady:()=>{clearTimeout(timer);status.textContent='Djuma’s current broadcast. Use the player controls to start watching or turn on sound.';},onStateChange:e=>{if(e.data===1){clearTimeout(timer);status.textContent='Watching Djuma Cam · sound and playback controls are in the player.';}if(e.data===0)fallback('This broadcast has ended. Check the official channel for the next stream.');},onError:()=>fallback('This broadcast is unavailable in the embedded player. Try the official YouTube channel below.')}});
 }catch{fallback('The live stream is unavailable here just now. The camera or connection may be offline.');}
});
