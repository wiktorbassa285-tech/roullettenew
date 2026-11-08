const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const page=()=>fs.readFileSync(path.join(__dirname,'public','index.html'));
const srv=http.createServer((q,r)=>{r.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});r.end(page())});
const wss=new WebSocketServer({server:srv,path:'/ws',maxPayload:16384});
const rooms=new Map();let n=0;
const send=(w,o)=>{if(w.readyState==1)w.send(JSON.stringify(o))};
const bcast=(r,o)=>{const s=rooms.get(r);if(s)s.forEach(w=>send(w,o))};
const pp=r=>{const s=rooms.get(r);if(s)bcast(r,{t:'peers',peers:[...s].map(w=>({peer:w.id,presence:w.pr}))})};
wss.on('connection',ws=>{
 ws.id='p'+(++n)+Math.random().toString(36).slice(2,6);ws.pr={};ws.room=null;ws.alive=true;
 ws.on('pong',()=>ws.alive=true);
 ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch{return}
  if(m.t=='join'&&!ws.room){const r=String(m.room).slice(0,40);if((rooms.get(r)||[]).size>=8){send(ws,{t:'full'});ws.close();return}ws.room=r;if(!rooms.has(ws.room))rooms.set(ws.room,new Set());rooms.get(ws.room).add(ws);send(ws,{t:'hi',id:ws.id});pp(ws.room)}
  else if(!ws.room)return;
  else if(m.t=='pres'&&m.patch&&typeof m.patch=='object'){const old=ws.pr;ws.pr={...old};for(const k in m.patch){if(m.patch[k]===null)delete ws.pr[k];else ws.pr[k]=m.patch[k]}
   if(JSON.stringify(ws.pr).length>5000){ws.pr=old;return}pp(ws.room)}
  else if(m.t=='emit'&&/^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic))bcast(ws.room,{t:'emit',topic:m.topic,data:m.data,peer:ws.id})});
 ws.on('close',()=>{const s=rooms.get(ws.room);if(s){s.delete(ws);if(!s.size)rooms.delete(ws.room);else pp(ws.room)}})});
setInterval(()=>wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()}),25000);
srv.listen(process.env.PORT||3000,()=>console.log('Neon Roulette: http://localhost:'+(process.env.PORT||3000)));
