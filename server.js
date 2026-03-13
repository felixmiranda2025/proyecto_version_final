// ================================================================
// VEF AUTOMATIZACIÓN — ERP Industrial
// server.js — Compatible con esquema existente en BD
// ================================================================
const express   = require('express');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool }  = require('pg');
const jwt       = require('jsonwebtoken');
const bcrypt    = require('bcryptjs');
const nodemailer= require('nodemailer');
const PDFKit    = require('pdfkit');
const path      = require('path');
const fs        = require('fs');
require('dotenv').config();

const app  = express();
const PORT = process.env.PORT || 3000;

const VEF_NOMBRE   = 'VEF Automatización';
const VEF_TELEFONO = '+52 (722) 115-7792';
const VEF_CORREO   = 'soporte.ventas@vef-automatizacion.com';

// Logo: buscar en carpeta raíz del proyecto
// Logo path — prioridad: 1) upload en caliente, 2) .env LOGO_FILE, 3) auto-búsqueda
function getLogoPath() {
  // 1. Upload realizado desde la pantalla de Configuración (sin reiniciar)
  if (global._logoPathOverride && fs.existsSync(global._logoPathOverride)) return global._logoPathOverride;
  // 2. Variable de entorno LOGO_FILE (puede ser ruta absoluta o relativa)
  if (process.env.LOGO_FILE) {
    const envPath = path.isAbsolute(process.env.LOGO_FILE)
      ? process.env.LOGO_FILE
      : path.join(__dirname, process.env.LOGO_FILE);
    if (fs.existsSync(envPath)) return envPath;
  }
  // 3. Auto-búsqueda en carpeta raíz y frontend/
  for (const n of ['logo.png','logo.PNG','logo.jpg','logo.JPG','logo.jpeg','Logo.png','Logo.jpg']) {
    const p = path.join(__dirname, n);
    if (fs.existsSync(p)) return p;
  }
  for (const n of ['logo.png','logo.PNG','logo.jpg','logo.JPG','logo.jpeg']) {
    const p = path.join(__dirname, 'frontend', n);
    if (fs.existsSync(p)) return p;
  }
  return '';
}
const LOGO_PATH = getLogoPath();

// ── DB ───────────────────────────────────────────────────────────
const pool = new Pool({
  host    : process.env.DB_HOST,
  port    : parseInt(process.env.DB_PORT) || 5432,
  database: process.env.DB_NAME || 'postgres',
  user    : process.env.DB_USER,
  password: process.env.DB_PASS,
  ssl     : { rejectUnauthorized: false },
  max: 20, idleTimeoutMillis:30000, connectionTimeoutMillis:10000,
});
pool.on('error', e => console.error('DB pool error:', e.message));

// Esquema real de la BD (se llena en autoSetup)
let DB = {};  // DB['tabla'] = ['col1','col2',...]

const has = (table, col) => (DB[table] || []).includes(col);

// Query seguro — nunca rompe el servidor
const Q = async (sql, p=[]) => {
  try { return (await pool.query(sql, p)).rows; }
  catch(e) { console.error('Query error:', e.message, '\n  SQL:', sql.slice(0,120)); return []; }
};

// ── MIDDLEWARE ───────────────────────────────────────────────────
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({ origin: '*', credentials: true }));
app.use(express.json({ limit: '20mb' }));
app.use(express.static(path.join(__dirname, 'frontend')));
app.use('/api', rateLimit({ windowMs:15*60*1000, max:2000 }));

// ── AUTH ─────────────────────────────────────────────────────────
const JWT_SECRET = process.env.JWT_SECRET || 'vef_secret_2025';
function auth(req, res, next) {
  // Accept token from header OR ?token= query param (for PDF window.open)
  const t = req.headers.authorization?.split(' ')[1] || req.query.token;
  if (!t) return res.status(401).json({ error: 'Token requerido' });
  try { req.user = jwt.verify(t, JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Token inválido' }); }
}
function adminOnly(req, res, next) {
  if (req.user?.rol !== 'admin') return res.status(403).json({ error: 'Solo admin' });
  next();
}

// ── EMAIL ────────────────────────────────────────────────────────
const mailer = nodemailer.createTransport({
  host: process.env.SMTP_HOST || 'smtp.zoho.com',
  port: parseInt(process.env.SMTP_PORT || '465'),
  secure: parseInt(process.env.SMTP_PORT || '465') === 465,
  auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  tls: { rejectUnauthorized: false, ciphers: 'SSLv3' }
});

// ================================================================
// PDF — con logo VEF si existe logo.png en la carpeta del proyecto
// ================================================================
const C = { AZUL:'#0D2B55', AZUL_MED:'#1A4A8A', AZUL_SUV:'#D6E4F7',
            GRIS:'#F4F6FA', GRIS_B:'#CCCCCC', BLANCO:'#FFFFFF', TEXTO:'#333333' };

function pdfHeader(doc, titulo, subs=[]) {
  const M=28, W=539, H=80;
  const _lp=getLogoPath();
  const hasLogo = !!_lp;
  if (hasLogo) {
    const LW = 120;
    doc.rect(M, 14, W, H).fill(C.AZUL);
    doc.rect(M, 14, LW, H).fill(C.BLANCO);
    try { doc.image(_lp, M+6, 18, { fit:[LW-12, H-8], align:'center', valign:'center' }); } catch(e){}
    doc.fillColor(C.BLANCO).fontSize(17).font('Helvetica-Bold')
       .text(titulo, M+LW+10, 22, { width: W-LW-14 });
    let ty=46; doc.fontSize(9).font('Helvetica');
    for (const s of subs) { doc.fillColor('#A8C5F0').text(s, M+LW+10, ty, { width:W-LW-14 }); ty+=12; }
  } else {
    doc.rect(M, 14, W, H).fill(C.AZUL);
    doc.fillColor(C.BLANCO).fontSize(18).font('Helvetica-Bold')
       .text(titulo, M+14, 22, { width:W-28, align:'center' });
    let ty=46; doc.fontSize(9).font('Helvetica').fillColor('#A8C5F0');
    for (const s of subs) { doc.text(s, M+14, ty, { width:W-28, align:'center' }); ty+=12; }
  }
  doc.y = 14 + H + 10;
}

function pdfWatermark(doc) {
  const _lp=getLogoPath(); if (!_lp) return;
  try { doc.save(); doc.opacity(0.07); doc.image(_lp, 158, 270, { fit:[280,280] }); doc.restore(); }
  catch(e){}
}

function pdfPie(doc) {
  const M=28, W=539;
  doc.moveDown(0.8);
  const y = Math.min(doc.y, 760);
  doc.moveTo(M,y).lineTo(M+W,y).lineWidth(1).strokeColor(C.AZUL_MED).stroke();
  const py = y+8;
  doc.rect(M,py,W,32).fill(C.AZUL);
  doc.fillColor(C.BLANCO).fontSize(9).font('Helvetica-Bold')
     .text(`${VEF_NOMBRE}   ·   📞 ${VEF_TELEFONO}   ·   ✉  ${VEF_CORREO}`, M, py+10, {width:W, align:'center'});
  doc.fillColor('#888').fontSize(8).font('Helvetica')
     .text(`Generado el ${new Date().toLocaleDateString('es-MX')} · ${VEF_NOMBRE}`, M, py+44, {width:W, align:'center'});
}

function pdfSec(doc, titulo) {
  const M=28, W=539;
  doc.moveDown(0.5);
  doc.fillColor(C.AZUL).fontSize(11).font('Helvetica-Bold').text(titulo, M);
  doc.moveDown(0.2);
  doc.moveTo(M,doc.y).lineTo(M+W,doc.y).lineWidth(1.5).strokeColor(C.AZUL_MED).stroke();
  doc.moveDown(0.4);
}

function pdfGrid(doc, filas) {
  const M=28, COLS=[84,163,84,163], H=20;
  let y=doc.y;
  for (const f of filas) {
    doc.rect(M,y,COLS[0]+COLS[1]+COLS[2]+COLS[3],H).fill(C.GRIS);
    doc.rect(M,y,COLS[0]+COLS[1]+COLS[2]+COLS[3],H).lineWidth(0.3).strokeColor(C.GRIS_B).stroke();
    let cx=M;
    for (let i=0;i<4;i++) {
      doc.fillColor(i%2===0?C.AZUL:C.TEXTO).fontSize(9)
         .font(i%2===0?'Helvetica-Bold':'Helvetica')
         .text(String(f[i]||''), cx+5, y+5, {width:COLS[i]-8,lineBreak:false});
      cx+=COLS[i];
    }
    y+=H;
  }
  doc.y=y+6;
}

function pdfItems(doc, items, moneda='USD') {
  const M=28,W=539,COLS=[280,56,98,105],SYM=moneda==='USD'?'$':'MX$';
  let y=doc.y;
  // Header
  doc.rect(M,y,W,22).fill(C.AZUL_MED);
  let cx=M;
  for (const [h,i] of [['Descripción',0],['Cant.',1],['P. Unitario',2],['Total '+moneda,3]]) {
    doc.fillColor(C.BLANCO).fontSize(9).font('Helvetica-Bold')
       .text(h, cx+5, y+6, {width:COLS[i]-8, align:i>0?'right':'left', lineBreak:false});
    cx+=COLS[i];
  }
  y+=22;
  if (!items.length) {
    doc.rect(M,y,W,20).fill(C.BLANCO);
    doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text('Sin partidas', M+6, y+5);
    y+=20;
  }
  for (let idx=0;idx<items.length;idx++) {
    const it=items[idx];
    const cant=parseFloat(it.cantidad||0), pu=parseFloat(it.precio_unitario||0);
    const tot=parseFloat(it.total||0)||cant*pu;
    doc.rect(M,y,W,20).fill(idx%2===0?C.AZUL_SUV:C.BLANCO);
    doc.rect(M,y,W,20).lineWidth(0.3).strokeColor(C.GRIS_B).stroke();
    cx=M;
    const vals=[it.descripcion||'', String(cant%1===0?cant:cant.toFixed(2)),
      SYM+pu.toLocaleString('es-MX',{minimumFractionDigits:2}),
      SYM+tot.toLocaleString('es-MX',{minimumFractionDigits:2})];
    for (let i=0;i<4;i++) {
      doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica')
         .text(vals[i], cx+5, y+5, {width:COLS[i]-8, align:i>0?'right':'left', lineBreak:false});
      cx+=COLS[i];
    }
    y+=20;
  }
  doc.y=y+6;
}

function pdfTotal(doc, label, total, moneda='USD') {
  const M=28,W=539,SYM=moneda==='USD'?'$':'MX$';
  const y=doc.y;
  doc.rect(M,y,W,28).fill(C.AZUL);
  doc.fillColor(C.BLANCO).fontSize(13).font('Helvetica-Bold')
     .text(`${label}:  ${SYM}${parseFloat(total||0).toLocaleString('es-MX',{minimumFractionDigits:2})} ${moneda}`,
       M+10, y+7, {width:W-20, align:'right'});
  doc.y=y+40;
}

function pdfCondiciones(doc, conds) {
  const M=28,W=539,LW=130;
  let y=doc.y;
  for (const [lbl,val] of conds) {
    if (!val||!String(val).trim()) continue;
    const txt=String(val).trim();
    const h=Math.max(20, Math.ceil(txt.length/85)*13 + txt.split('\n').length*13);
    doc.rect(M,y,LW,h).fill(C.AZUL_SUV);
    doc.rect(M+LW,y,W-LW,h).fill(C.BLANCO);
    doc.rect(M,y,W,h).lineWidth(0.3).strokeColor(C.GRIS_B).stroke();
    doc.fillColor(C.AZUL).fontSize(9).font('Helvetica-Bold').text(lbl, M+5, y+5, {width:LW-8,lineBreak:false});
    doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(txt, M+LW+5, y+5, {width:W-LW-8});
    y+=h; doc.y=y;
  }
  doc.y=y+6;
}

async function buildPDFCotizacion(cot, items) {
  return new Promise((res,rej)=>{
    const doc=new PDFKit({margin:28,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);
    pdfWatermark(doc);
    pdfHeader(doc,'COTIZACIÓN COMERCIAL',[
      `No. ${cot.numero_cotizacion||'—'}  |  Fecha: ${fmt(cot.fecha_emision||cot.created_at)}  |  Válida hasta: ${fmt(cot.validez_hasta)||'N/A'}`,
      `Proyecto: ${cot.proyecto_nombre||'—'}`
    ]);
    pdfSec(doc,'Información del Cliente');
    pdfGrid(doc,[
      ['Empresa:', cot.cliente_nombre||'—', 'Contacto:', cot.cliente_contacto||'—'],
      ['Dirección:',cot.cliente_dir||'—',   'Email:',    cot.cliente_email||'—'],
      ['Teléfono:', cot.cliente_tel||'—',   'RFC:',      cot.cliente_rfc||'—'],
    ]);
    if (cot.alcance_tecnico) {
      pdfSec(doc,'Alcance Técnico');
      doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(cot.alcance_tecnico,28,doc.y,{width:539});
      doc.moveDown(0.5);
    }
    pdfSec(doc,'Detalle de Partidas / Precios');
    pdfItems(doc, items, cot.moneda||'USD');
    pdfTotal(doc,'TOTAL GENERAL', cot.total, cot.moneda||'USD');
    const conds=[
      ['Condiciones de Entrega y Pago', cot.condiciones_pago||cot.condiciones_entrega],
      ['Garantía y Responsabilidad',    cot.garantia],
      ['Servicio Postventa',            cot.servicio_postventa],
      ['Notas Importantes',             cot.notas_importantes],
      ['Comentarios Generales',         cot.comentarios_generales],
      ['Validez',                       cot.validez],
      ['Fuerza Mayor',                  cot.fuerza_mayor],
      ['Ley Aplicable',                 cot.ley_aplicable],
    ];
    if (conds.some(([,v])=>v)) { pdfSec(doc,'Términos y Condiciones'); pdfCondiciones(doc,conds); }
    pdfPie(doc); doc.end();
  });
}

async function buildPDFOrden(oc, items) {
  return new Promise((res,rej)=>{
    const doc=new PDFKit({margin:28,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);
    pdfWatermark(doc);
    pdfHeader(doc,'ORDEN DE COMPRA',[
      `No. ${oc.numero_op||oc.numero_oc||'—'}  |  Emisión: ${fmt(oc.fecha_emision||oc.created_at)}  |  Entrega: ${fmt(oc.fecha_entrega)||'Por definir'}`,
    ]);
    pdfSec(doc,'Datos del Proveedor');
    pdfGrid(doc,[
      ['Proveedor:', oc.proveedor_nombre||'—', 'Contacto:', oc.proveedor_contacto||'—'],
      ['Dirección:', oc.proveedor_dir||'—',    'Email:',    oc.proveedor_email||'—'],
      ['Teléfono:',  oc.proveedor_tel||'—',    'RFC:',      oc.proveedor_rfc||'—'],
    ]);
    pdfSec(doc,'Condiciones');
    pdfGrid(doc,[['Cond. Pago:', oc.condiciones_pago||'—','Lugar de Entrega:',oc.lugar_entrega||'—']]);
    pdfSec(doc,'Partidas / Materiales');
    pdfItems(doc, items, oc.moneda||'USD');
    pdfTotal(doc,'TOTAL ORDEN', oc.total, oc.moneda||'USD');
    if (oc.notas) { pdfSec(doc,'Notas'); doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(oc.notas,28,doc.y,{width:539}); doc.moveDown(0.5); }
    // Firmas
    doc.moveDown(1.2);
    const fy=doc.y;
    doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica')
       .text('_______________________________',28,fy,{width:240,align:'center'})
       .text('_______________________________',299,fy,{width:240,align:'center'});
    doc.moveDown(0.3);
    doc.font('Helvetica-Bold')
       .text(`Autorizado: ${VEF_NOMBRE}`,28,doc.y,{width:240,align:'center'})
       .text(`Proveedor: ${oc.proveedor_nombre||'—'}`,299,doc.y,{width:240,align:'center'});
    pdfPie(doc); doc.end();
  });
}

async function buildPDFFactura(f, items=[]) {
  return new Promise((res,rej)=>{
    const doc=new PDFKit({margin:28,size:'A4'});
    const ch=[]; doc.on('data',c=>ch.push(c)); doc.on('end',()=>res(Buffer.concat(ch))); doc.on('error',rej);
    pdfWatermark(doc);
    pdfHeader(doc,'FACTURA',[
      `No. ${f.numero_factura||'—'}  |  Fecha: ${fmt(f.fecha_emision)}  |  Estatus: ${(f.estatus||'pendiente').toUpperCase()}`,
    ]);
    pdfSec(doc,'Datos del Cliente');
    pdfGrid(doc,[
      ['Cliente:', f.cliente_nombre||'—', 'RFC:', f.cliente_rfc||'—'],
      ['Email:',   f.cliente_email||'—',  'Tel:', f.cliente_tel||'—'],
    ]);
    if (items.length) { pdfSec(doc,'Detalle'); pdfItems(doc,items,f.moneda||'USD'); }
    const M=28,W=539,SYM=(f.moneda||'USD')==='USD'?'$':'MX$';
    const sub=parseFloat(f.subtotal||f.monto||f.total||0), iva=parseFloat(f.iva||0);
    doc.fillColor(C.TEXTO).fontSize(10).font('Helvetica')
       .text(`Subtotal: ${SYM}${sub.toLocaleString('es-MX',{minimumFractionDigits:2})}`,M,doc.y,{width:W,align:'right'})
       .text(`IVA: ${SYM}${iva.toLocaleString('es-MX',{minimumFractionDigits:2})}`,M,doc.y,{width:W,align:'right'});
    doc.moveDown(0.3);
    pdfTotal(doc,'TOTAL FACTURA', f.total||f.monto, f.moneda||'USD');
    if (f.fecha_vencimiento) doc.fillColor(C.AZUL).fontSize(9).font('Helvetica-Bold')
       .text(`Vencimiento: ${fmt(f.fecha_vencimiento)}`,M,doc.y,{width:W});
    if (f.notas) { doc.moveDown(0.3); doc.fillColor(C.TEXTO).fontSize(9).font('Helvetica').text(f.notas,M,doc.y,{width:W}); }
    pdfPie(doc); doc.end();
  });
}

function fmt(v) {
  if (!v) return '—';
  try { return new Date(v).toLocaleDateString('es-MX',{day:'2-digit',month:'2-digit',year:'numeric'}); }
  catch { return String(v).slice(0,10); }
}

// ================================================================
// HEALTH
// ================================================================
app.get('/api/health', async (req,res) => {
  const t=Date.now();
  try {
    const [{db,u,ts,tabs}] = (await pool.query(
      `SELECT current_database() db, current_user u, NOW() ts,
       (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public') tabs`)).rows;
    res.json({status:'ok',connected:true,latency_ms:Date.now()-t,database:db,server_time:ts,
      total_tables:parseInt(tabs), logo:LOGO_PATH?'✅ '+path.basename(LOGO_PATH):'❌ no encontrado'});
  } catch(e){ res.status(503).json({status:'error',connected:false,error:e.message}); }
});

app.get('/api/setup', async (req,res)=>{ await autoSetup(); res.json({ok:true}); });

// ================================================================
// AUTO SETUP — se adapta al esquema REAL de la BD
// ================================================================
async function autoSetup() {
  try {
    // 1. Leer esquema real
    const {rows} = await pool.query(`
      SELECT t.table_name, c.column_name
      FROM information_schema.tables t
      JOIN information_schema.columns c ON c.table_name=t.table_name AND c.table_schema='public'
      WHERE t.table_schema='public' AND t.table_type='BASE TABLE'
      ORDER BY t.table_name, c.ordinal_position`);
    DB={};
    for (const r of rows) { if(!DB[r.table_name]) DB[r.table_name]=[]; DB[r.table_name].push(r.column_name); }
    global.dbSchema=DB;
    console.log('📦 Tablas:', Object.keys(DB).join(', ')||'ninguna');

    // 2. Crear SOLO las tablas que no existen
    const TABLES={
      usuarios:`CREATE TABLE IF NOT EXISTS usuarios (
        id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE NOT NULL,
        nombre VARCHAR(100), password_hash TEXT NOT NULL,
        rol VARCHAR(20) DEFAULT 'usuario', activo BOOLEAN DEFAULT true,
        ultimo_acceso TIMESTAMP, created_at TIMESTAMP DEFAULT NOW())`,
      clientes:`CREATE TABLE IF NOT EXISTS clientes (
        id SERIAL PRIMARY KEY, nombre TEXT NOT NULL, contacto TEXT,
        direccion TEXT, telefono TEXT, email TEXT, rfc TEXT,
        created_at TIMESTAMP DEFAULT NOW())`,
      proveedores:`CREATE TABLE IF NOT EXISTS proveedores (
        id SERIAL PRIMARY KEY, nombre TEXT NOT NULL, contacto TEXT,
        direccion TEXT, telefono TEXT, email TEXT, rfc TEXT,
        condiciones_pago TEXT, created_at TIMESTAMP DEFAULT NOW())`,
      proyectos:`CREATE TABLE IF NOT EXISTS proyectos (
        id SERIAL PRIMARY KEY, nombre TEXT NOT NULL, cliente_id INTEGER,
        responsable TEXT DEFAULT 'VEF Automatización',
        fecha_creacion DATE DEFAULT CURRENT_DATE, estatus TEXT DEFAULT 'activo',
        created_at TIMESTAMP DEFAULT NOW())`,
      cotizaciones:`CREATE TABLE IF NOT EXISTS cotizaciones (
        id SERIAL PRIMARY KEY, proyecto_id INTEGER, numero_cotizacion TEXT UNIQUE,
        fecha_emision DATE DEFAULT CURRENT_DATE, validez_hasta DATE,
        alcance_tecnico TEXT, notas_importantes TEXT, comentarios_generales TEXT,
        servicio_postventa TEXT, condiciones_entrega TEXT, condiciones_pago TEXT,
        garantia TEXT, responsabilidad TEXT, validez TEXT, fuerza_mayor TEXT,
        ley_aplicable TEXT, total NUMERIC(15,2) DEFAULT 0,
        moneda TEXT DEFAULT 'USD', estatus TEXT DEFAULT 'borrador',
        created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`,
      items_cotizacion:`CREATE TABLE IF NOT EXISTS items_cotizacion (
        id SERIAL PRIMARY KEY, cotizacion_id INTEGER, descripcion TEXT,
        cantidad NUMERIC(10,2), precio_unitario NUMERIC(15,2), total NUMERIC(15,2))`,
      seguimientos:`CREATE TABLE IF NOT EXISTS seguimientos (
        id SERIAL PRIMARY KEY, cotizacion_id INTEGER,
        fecha TIMESTAMP DEFAULT NOW(), tipo TEXT, notas TEXT, proxima_accion TEXT)`,
      ordenes_proveedor:`CREATE TABLE IF NOT EXISTS ordenes_proveedor (
        id SERIAL PRIMARY KEY, proveedor_id INTEGER, numero_op TEXT UNIQUE,
        fecha_emision DATE DEFAULT CURRENT_DATE, fecha_entrega DATE,
        condiciones_pago TEXT, lugar_entrega TEXT, notas TEXT,
        total NUMERIC(15,2) DEFAULT 0, moneda TEXT DEFAULT 'USD',
        estatus TEXT DEFAULT 'borrador', cotizacion_ref_pdf TEXT,
        created_by INTEGER, created_at TIMESTAMP DEFAULT NOW())`,
      items_orden_proveedor:`CREATE TABLE IF NOT EXISTS items_orden_proveedor (
        id SERIAL PRIMARY KEY, orden_id INTEGER, descripcion TEXT,
        cantidad NUMERIC(10,2), precio_unitario NUMERIC(15,2), total NUMERIC(15,2))`,
      seguimientos_oc:`CREATE TABLE IF NOT EXISTS seguimientos_oc (
        id SERIAL PRIMARY KEY, orden_id INTEGER,
        fecha TIMESTAMP DEFAULT NOW(), tipo TEXT, notas TEXT, proxima_accion TEXT)`,
      facturas:`CREATE TABLE IF NOT EXISTS facturas (
        id SERIAL PRIMARY KEY, cotizacion_id INTEGER, numero_factura TEXT,
        cliente_id INTEGER, moneda TEXT DEFAULT 'USD',
        subtotal NUMERIC(15,2) DEFAULT 0, iva NUMERIC(15,2) DEFAULT 0,
        total NUMERIC(15,2) DEFAULT 0, monto NUMERIC(15,2) DEFAULT 0,
        fecha_emision DATE DEFAULT CURRENT_DATE, fecha_vencimiento DATE,
        estatus TEXT DEFAULT 'pendiente', estatus_pago TEXT DEFAULT 'pendiente',
        notas TEXT, created_at TIMESTAMP DEFAULT NOW())`,
      pagos:`CREATE TABLE IF NOT EXISTS pagos (
        id SERIAL PRIMARY KEY, factura_id INTEGER,
        fecha DATE DEFAULT CURRENT_DATE, monto NUMERIC(15,2),
        metodo TEXT, referencia TEXT, notas TEXT)`,
      inventario:`CREATE TABLE IF NOT EXISTS inventario (
        id SERIAL PRIMARY KEY, codigo TEXT, nombre TEXT NOT NULL,
        descripcion TEXT, categoria TEXT, unidad TEXT DEFAULT 'pza',
        cantidad_actual NUMERIC(10,2) DEFAULT 0, cantidad_minima NUMERIC(10,2) DEFAULT 0,
        precio_costo NUMERIC(15,2) DEFAULT 0, precio_venta NUMERIC(15,2) DEFAULT 0,
        ubicacion TEXT, proveedor_id INTEGER,
        fecha_ultima_entrada DATE, notas TEXT, created_at TIMESTAMP DEFAULT NOW())`,
      movimientos_inventario:`CREATE TABLE IF NOT EXISTS movimientos_inventario (
        id SERIAL PRIMARY KEY, producto_id INTEGER,
        fecha TIMESTAMP DEFAULT NOW(), tipo TEXT, cantidad NUMERIC(10,2),
        stock_anterior NUMERIC(10,2), stock_nuevo NUMERIC(10,2),
        referencia TEXT, notas TEXT, created_by INTEGER)`,
      empresa_config:`CREATE TABLE IF NOT EXISTS empresa_config (
        id SERIAL PRIMARY KEY,
        nombre VARCHAR(200) NOT NULL DEFAULT 'VEF Automatización',
        razon_social VARCHAR(200), rfc VARCHAR(30), regimen_fiscal VARCHAR(100),
        contacto VARCHAR(100), telefono VARCHAR(50), email VARCHAR(100),
        direccion TEXT, ciudad VARCHAR(100), estado VARCHAR(100), cp VARCHAR(10),
        pais VARCHAR(50) DEFAULT 'México', sitio_web VARCHAR(150),
        moneda_default VARCHAR(10) DEFAULT 'USD', iva_default NUMERIC(5,2) DEFAULT 16.00,
        notas_factura TEXT, notas_cotizacion TEXT,
        updated_at TIMESTAMP DEFAULT NOW())`,
    };
    for (const [name,sql] of Object.entries(TABLES)) {
      await pool.query(sql);
      if (!DB[name]) console.log(`  ✅ Tabla creada: ${name}`);
    }

    // 3. Agregar columnas faltantes a tablas EXISTENTES
    const ALTER = async (table, col, def) => {
      if ((DB[table]||[]).length>0 && !has(table,col)) {
        try {
          await pool.query(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${col} ${def}`);
          console.log(`  ✅ Columna agregada: ${table}.${col}`);
          if (!DB[table]) DB[table]=[];
          DB[table].push(col);
        } catch(e){ console.log(`  ⚠ No se pudo agregar ${table}.${col}: ${e.message}`); }
      }
    };

    // Usuarios
    await ALTER('usuarios','password_hash','TEXT');
    await ALTER('usuarios','rol',"TEXT DEFAULT 'usuario'");
    await ALTER('usuarios','activo','BOOLEAN DEFAULT true');
    await ALTER('usuarios','ultimo_acceso','TIMESTAMP');
    await ALTER('usuarios','email','TEXT');
    await ALTER('usuarios','created_at','TIMESTAMP DEFAULT NOW()');
    // Clientes
    await ALTER('clientes','rfc','TEXT');
    await ALTER('clientes','activo','BOOLEAN DEFAULT true');
    await ALTER('clientes','created_at','TIMESTAMP DEFAULT NOW()');
    // Proveedores
    await ALTER('proveedores','activo','BOOLEAN DEFAULT true');
    await ALTER('proveedores','created_at','TIMESTAMP DEFAULT NOW()');
    // Proyectos
    await ALTER('proyectos','created_at','TIMESTAMP DEFAULT NOW()');
    await ALTER('proyectos','responsable',"TEXT DEFAULT 'VEF Automatización'");
    // Cotizaciones
    await ALTER('cotizaciones','created_at','TIMESTAMP DEFAULT NOW()');
    await ALTER('cotizaciones','updated_at','TIMESTAMP DEFAULT NOW()');
    await ALTER('cotizaciones','servicio_postventa','TEXT');
    await ALTER('cotizaciones','condiciones_entrega','TEXT');
    await ALTER('cotizaciones','responsabilidad','TEXT');
    await ALTER('cotizaciones','validez','TEXT');
    await ALTER('cotizaciones','fuerza_mayor','TEXT');
    await ALTER('cotizaciones','ley_aplicable','TEXT');
    await ALTER('cotizaciones','created_by','INTEGER');
    await ALTER('cotizaciones','moneda',"TEXT DEFAULT 'USD'");
    // Facturas
    await ALTER('facturas','cliente_id','INTEGER');
    await ALTER('facturas','subtotal','NUMERIC(15,2) DEFAULT 0');
    await ALTER('facturas','iva','NUMERIC(15,2) DEFAULT 0');
    await ALTER('facturas','total','NUMERIC(15,2) DEFAULT 0');
    await ALTER('facturas','moneda',"TEXT DEFAULT 'USD'");
    await ALTER('facturas','fecha_vencimiento','DATE');
    await ALTER('facturas','notas','TEXT');
    await ALTER('facturas','created_at','TIMESTAMP DEFAULT NOW()');
    // Inventario (puede llamarse stock_actual en vez de cantidad_actual)
    await ALTER('inventario','activo','BOOLEAN DEFAULT true');
    await ALTER('inventario','created_at','TIMESTAMP DEFAULT NOW()');
    await ALTER('inventario','cantidad_actual','NUMERIC(10,2) DEFAULT 0');
    await ALTER('inventario','cantidad_minima','NUMERIC(10,2) DEFAULT 0');
    // Si existe stock_actual pero no cantidad_actual, sincronizar
    if (has('inventario','stock_actual') && has('inventario','cantidad_actual')) {
      await Q("UPDATE inventario SET cantidad_actual=COALESCE(stock_actual,0) WHERE cantidad_actual=0 AND stock_actual>0");
    }
    // Pagos
    await ALTER('pagos','fecha','TIMESTAMP DEFAULT NOW()');
    await ALTER('pagos','metodo','TEXT');
    await ALTER('pagos','referencia','TEXT');
    await ALTER('pagos','notas','TEXT');
    await ALTER('pagos','created_at','TIMESTAMP DEFAULT NOW()');
    // Movimientos inventario — columnas que puede tener la BD existente
    await ALTER('movimientos_inventario','stock_anterior','NUMERIC(10,2) DEFAULT 0');
    await ALTER('movimientos_inventario','stock_nuevo','NUMERIC(10,2) DEFAULT 0');
    await ALTER('movimientos_inventario','referencia','TEXT');
    await ALTER('movimientos_inventario','notas','TEXT');
    await ALTER('movimientos_inventario','created_by','INTEGER');
    // Ordenes proveedor
    await ALTER('ordenes_proveedor','moneda',"TEXT DEFAULT 'USD'");
    await ALTER('ordenes_proveedor','total','NUMERIC(15,2) DEFAULT 0');
    await ALTER('ordenes_proveedor','created_by','INTEGER');

    // Insertar registro por defecto en empresa_config si está vacío
    try {
      const ec = await pool.query('SELECT id FROM empresa_config LIMIT 1');
      if (ec.rows.length === 0) {
        await pool.query(`INSERT INTO empresa_config
          (nombre,razon_social,rfc,regimen_fiscal,telefono,email,direccion,ciudad,estado,cp,pais,moneda_default,iva_default)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
          ['VEF Automatización','VEF Automatización S.A. de C.V.','','',
           '+52 (722) 115-7792','soporte.ventas@vef-automatizacion.com',
           '','','Estado de México','','México','USD',16.00]);
        console.log('  ✅ Empresa configurada por defecto');
      }
    } catch(e) { console.log('  ⚠ empresa_config init:', e.message); }

    // 4. Volver a leer esquema actualizado
    const {rows:rows2} = await pool.query(`
      SELECT t.table_name, c.column_name
      FROM information_schema.tables t
      JOIN information_schema.columns c ON c.table_name=t.table_name AND c.table_schema='public'
      WHERE t.table_schema='public' AND t.table_type='BASE TABLE'
      ORDER BY t.table_name, c.ordinal_position`);
    DB={};
    for (const r of rows2) { if(!DB[r.table_name]) DB[r.table_name]=[]; DB[r.table_name].push(r.column_name); }
    global.dbSchema=DB;

    // 5. Crear usuarios por defecto
    const USERS=[
      {username:'admin',    nombre:'Administrador',        rol:'admin',   pass:'admin123'},
      {username:'ventas',   nombre:'Ejecutivo de Ventas',  rol:'ventas',  pass:'ventas123'},
      {username:'compras',  nombre:'Agente de Compras',    rol:'compras', pass:'compras123'},
      {username:'almacen',  nombre:'Encargado Almacén',    rol:'almacen', pass:'almacen123'},
      {username:'gerencia', nombre:'Gerencia General',     rol:'admin',   pass:'gerencia123'},
    ];
    for (const u of USERS) {
      const hash=await bcrypt.hash(u.pass,12);
      const chk=await pool.query("SELECT id FROM usuarios WHERE username=$1 LIMIT 1",[u.username]);
      if (chk.rows.length===0) {
        // Construir INSERT dinámico según columnas disponibles
        const cols=['username','nombre','rol'];
        const vals=[u.username,u.nombre,u.rol];
        if (has('usuarios','password_hash')){ cols.push('password_hash'); vals.push(hash); }
        if (has('usuarios','password'))     { cols.push('password');      vals.push(hash); }
        if (has('usuarios','contrasena'))   { cols.push('contrasena');    vals.push(hash); }
        if (has('usuarios','activo')){ cols.push('activo'); vals.push(true); }
        if (has('usuarios','email')){ cols.push('email'); vals.push(`${u.username}@vef.com`); }
        const ph=vals.map((_,i)=>`$${i+1}`).join(',');
        await pool.query(`INSERT INTO usuarios (${cols.join(',')}) VALUES (${ph})`,vals);
        console.log(`  ✅ Usuario: ${u.username} / ${u.pass}`);
      } else if (u.username==='admin') {
        // Actualizar todas las columnas de contraseña del admin
        const asets=[];const avals=[];let ai=1;
        if(has('usuarios','password_hash')){asets.push(`password_hash=$${ai++}`);avals.push(hash);}
        if(has('usuarios','password')){asets.push(`password=$${ai++}`);avals.push(hash);}
        if(has('usuarios','contrasena')){asets.push(`contrasena=$${ai++}`);avals.push(hash);}
        avals.push('admin');
        if(asets.length) await pool.query(`UPDATE usuarios SET ${asets.join(',')},rol='admin' WHERE username=$${ai}`,avals);
        console.log('  ✅ Admin: admin / admin123');
      }
    }

    if (LOGO_PATH) console.log(`\n🖼  Logo: ${path.basename(LOGO_PATH)}`);
    else console.log('\n⚠  Sin logo — coloca logo.png junto a server.js');

    console.log('\n👥 Usuarios:');
    console.log('   admin / admin123 · ventas / ventas123 · compras / compras123');
    console.log('   almacen / almacen123 · gerencia / gerencia123\n');

  } catch(e){ console.error('⚠ Setup error:', e.message); }
}

// ── Helper: columnas seguras para SELECT ─────────────────────────
// Construye SELECT * pero omite columnas que no existen
function safeSelect(table, alias='') {
  const a = alias ? alias+'.' : '';
  // Devuelve * si no tenemos el esquema todavía
  return `${a}*`;
}

// ================================================================
// AUTH
// ================================================================
app.post('/api/auth/login', async (req,res) => {
  const {username,password}=req.body;
  try {
    const result=await pool.query('SELECT * FROM usuarios WHERE username=$1',[username]);
    const user=result.rows[0];
    if (!user) return res.status(401).json({error:'Usuario no encontrado'});
    // Verificar activo si la columna existe
    if (has('usuarios','activo') && user.activo===false)
      return res.status(401).json({error:'Usuario desactivado'});
    const hash=user.password_hash||user.password||user.contrasena||'';
    if (!hash) return res.status(401).json({error:'Sin contraseña. Reinicia el servidor.'});
    if (!await bcrypt.compare(password,hash)) return res.status(401).json({error:'Contraseña incorrecta'});
    if (has('usuarios','ultimo_acceso'))
      try { await pool.query('UPDATE usuarios SET ultimo_acceso=NOW() WHERE id=$1',[user.id]); } catch{}
    const token=jwt.sign({id:user.id,username:user.username,nombre:user.nombre,rol:user.rol||'usuario'},JWT_SECRET,{expiresIn:'8h'});
    res.json({token,user:{id:user.id,nombre:user.nombre,username:user.username,rol:user.rol||'usuario'}});
  } catch(e){ res.status(500).json({error:'Error: '+e.message}); }
});

app.post('/api/auth/change-password', auth, async (req,res)=>{
  try {
    const {password_actual,password_nuevo}=req.body;
    const [u]=await Q('SELECT * FROM usuarios WHERE id=$1',[req.user.id]);
    if (!u) return res.status(404).json({error:'Usuario no encontrado'});
    const h=u.password_hash||u.password||'';
    if (!await bcrypt.compare(password_actual,h)) return res.status(401).json({error:'Contraseña actual incorrecta'});
    const newHash=await bcrypt.hash(password_nuevo,12);
    const csets=[];const cvals=[];let ci=1;
    if(has('usuarios','password_hash')){csets.push(`password_hash=$${ci++}`);cvals.push(newHash);}
    if(has('usuarios','password')){csets.push(`password=$${ci++}`);cvals.push(newHash);}
    if(has('usuarios','contrasena')){csets.push(`contrasena=$${ci++}`);cvals.push(newHash);}
    cvals.push(req.user.id);
    await pool.query(`UPDATE usuarios SET ${csets.join(',')} WHERE id=$${ci}`,cvals);
    res.json({ok:true});
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// DASHBOARD
// ================================================================
app.get('/api/dashboard/metrics', auth, async (req,res)=>{
  const n=async q=>{try{return parseInt((await pool.query(q)).rows[0]?.count||0);}catch{return 0;}};
  const s=async q=>{try{return parseFloat((await pool.query(q)).rows[0]?.sum||0);}catch{return 0;}};
  const activoClientes   = has('clientes','activo')   ? 'WHERE activo=true' : '';
  const activoProveedores= has('proveedores','activo') ? 'WHERE activo=true' : '';
  const activoInventario = has('inventario','activo')  ? 'WHERE activo=true' : '';
  const [cots,clts,provs,prods,facts,ocs]=await Promise.all([
    n('SELECT COUNT(*) FROM cotizaciones'),
    n(`SELECT COUNT(*) FROM clientes ${activoClientes}`),
    n(`SELECT COUNT(*) FROM proveedores ${activoProveedores}`),
    n(`SELECT COUNT(*) FROM inventario ${activoInventario}`),
    n("SELECT COUNT(*) FROM facturas WHERE estatus='pendiente' OR estatus_pago='pendiente'"),
    n("SELECT COUNT(*) FROM ordenes_proveedor WHERE estatus NOT IN ('recibida','cancelada')"),
  ]);
  res.json({cotizaciones:cots,clientes:clts,proveedores:provs,
    inventario:prods,facturas_pendientes:facts,ordenes_activas:ocs});
});

// ================================================================
// REPORTES
// ================================================================
app.get('/api/reportes/cotizaciones', auth, async (req,res)=>{
  const resumen=await Q('SELECT estatus,COUNT(*) cantidad,COALESCE(SUM(total),0) total FROM cotizaciones GROUP BY estatus');
  const detalle=await Q(`
    SELECT c.numero_cotizacion, cl.nombre cliente, c.total, c.estatus,
           TO_CHAR(COALESCE(c.created_at,NOW()),'DD/MM/YYYY') fecha
    FROM cotizaciones c
    JOIN proyectos p ON c.proyecto_id=p.id
    JOIN clientes cl ON p.cliente_id=cl.id
    ORDER BY c.id DESC LIMIT 20`);
  res.json({resumen,detalle});
});
app.get('/api/reportes/facturas-pendientes', auth, async (req,res)=>{
  const estCol=has('facturas','estatus_pago')?'f.estatus_pago':'f.estatus';
  res.json(await Q(`
    SELECT f.numero_factura, COALESCE(f.total,f.monto,0) monto, ${estCol} estatus,
           TO_CHAR(f.fecha_emision,'DD/MM/YYYY') fecha
    FROM facturas f WHERE ${estCol}='pendiente' ORDER BY f.id DESC`));
});
app.get('/api/reportes/proyectos-activos', auth, async (req,res)=>{
  const respCol=has('proyectos','responsable')?"p.responsable":"'VEF Automatización'";
  res.json(await Q(`
    SELECT p.nombre, c.nombre cliente, ${respCol} responsable,
           TO_CHAR(COALESCE(p.fecha_creacion,NOW()::date),'DD/MM/YYYY') fecha
    FROM proyectos p JOIN clientes c ON p.cliente_id=c.id
    WHERE p.estatus='activo' ORDER BY p.nombre`));
});
app.get('/api/reportes/inventario-bajo', auth, async (req,res)=>{
  const cantCol=has('inventario','cantidad_actual')?'cantidad_actual':'COALESCE(stock_actual,0)';
  const minCol =has('inventario','cantidad_minima')?'cantidad_minima':'COALESCE(stock_minimo,0)';
  const actFil =has('inventario','activo')?'WHERE activo=true':'';
  res.json(await Q(`
    SELECT nombre,categoria,unidad,${cantCol} cantidad_actual,${minCol} cantidad_minima,
           precio_costo,precio_venta,ubicacion,
           CASE WHEN ${cantCol}<=${minCol} THEN 'BAJO' ELSE 'OK' END estado
    FROM inventario ${actFil} ORDER BY nombre`));
});
app.get('/api/reportes/facturas-por-vencer', auth, async (req,res)=>{
  const estCol=has('facturas','estatus_pago')?'f.estatus_pago':'f.estatus';
  res.json(await Q(`
    SELECT f.numero_factura, COALESCE(f.total,f.monto,0) monto, f.moneda,
           TO_CHAR(f.fecha_vencimiento,'DD/MM/YYYY') vencimiento,
           ${estCol} estatus,
           COALESCE((SELECT SUM(p.monto) FROM pagos p WHERE p.factura_id=f.id),0) pagado,
           (f.fecha_vencimiento-CURRENT_DATE) dias
    FROM facturas f
    WHERE ${estCol}!='pagada'
      AND f.fecha_vencimiento IS NOT NULL
      AND f.fecha_vencimiento<=CURRENT_DATE+30
    ORDER BY f.fecha_vencimiento`));
});

// ================================================================
// CLIENTES
// ================================================================
app.get('/api/clientes', auth, async (req,res)=>{
  const w=has('clientes','activo')?'WHERE activo=true':'';
  res.json(await Q(`SELECT * FROM clientes ${w} ORDER BY nombre`));
});
app.get('/api/clientes/:id', auth, async (req,res)=>{
  const r=await Q('SELECT * FROM clientes WHERE id=$1',[req.params.id]);
  r[0]?res.json(r[0]):res.status(404).json({error:'No encontrado'});
});
app.post('/api/clientes', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc}=req.body;
    const cols=['nombre','contacto','direccion','telefono','email'];
    const vals=[nombre,contacto,direccion,telefono,email];
    if(has('clientes','rfc')){cols.push('rfc');vals.push(rfc?.toUpperCase());}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows}=await pool.query(`INSERT INTO clientes (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/clientes/:id', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc}=req.body;
    const sets=[]; const vals=[];let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    add('nombre',nombre);add('contacto',contacto);add('direccion',direccion);
    add('telefono',telefono);add('email',email);
    if(has('clientes','rfc')) add('rfc',rfc?.toUpperCase());
    vals.push(req.params.id);
    const {rows}=await pool.query(`UPDATE clientes SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/clientes/:id', auth, adminOnly, async (req,res)=>{
  if(has('clientes','activo')) await Q('UPDATE clientes SET activo=false WHERE id=$1',[req.params.id]);
  else await Q('DELETE FROM clientes WHERE id=$1',[req.params.id]);
  res.json({ok:true});
});

// ================================================================
// PROVEEDORES
// ================================================================
app.get('/api/proveedores', auth, async (req,res)=>{
  const w=has('proveedores','activo')?'WHERE activo=true':'';
  res.json(await Q(`SELECT * FROM proveedores ${w} ORDER BY nombre`));
});
app.post('/api/proveedores', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc,condiciones_pago}=req.body;
    const cols=['nombre','contacto','direccion','telefono','email'];
    const vals=[nombre,contacto,direccion,telefono,email];
    if(has('proveedores','rfc')){cols.push('rfc');vals.push(rfc?.toUpperCase());}
    if(has('proveedores','condiciones_pago')){cols.push('condiciones_pago');vals.push(condiciones_pago);}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows}=await pool.query(`INSERT INTO proveedores (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/proveedores/:id', auth, async (req,res)=>{
  try {
    const {nombre,contacto,direccion,telefono,email,rfc,condiciones_pago}=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    add('nombre',nombre);add('contacto',contacto);add('direccion',direccion);
    add('telefono',telefono);add('email',email);
    if(has('proveedores','rfc')) add('rfc',rfc?.toUpperCase());
    if(has('proveedores','condiciones_pago')) add('condiciones_pago',condiciones_pago);
    vals.push(req.params.id);
    const {rows}=await pool.query(`UPDATE proveedores SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/proveedores/:id', auth, adminOnly, async (req,res)=>{
  if(has('proveedores','activo')) await Q('UPDATE proveedores SET activo=false WHERE id=$1',[req.params.id]);
  else await Q('DELETE FROM proveedores WHERE id=$1',[req.params.id]);
  res.json({ok:true});
});

// ================================================================
// PROYECTOS
// ================================================================
app.get('/api/proyectos', auth, async (req,res)=>{
  const respCol=has('proyectos','responsable')?"p.responsable,":"";
  res.json(await Q(`
    SELECT p.id,p.nombre,p.cliente_id,${respCol}p.estatus,
           COALESCE(p.fecha_creacion,p.created_at) fecha,
           c.nombre cliente_nombre
    FROM proyectos p LEFT JOIN clientes c ON c.id=p.cliente_id
    ORDER BY p.id DESC`));
});
app.post('/api/proyectos', auth, async (req,res)=>{
  try {
    const {nombre,cliente_id,responsable,estatus}=req.body;
    const cols=['nombre','cliente_id','estatus'];
    const vals=[nombre,cliente_id||null,estatus||'activo'];
    if(has('proyectos','responsable')){cols.push('responsable');vals.push(responsable||'VEF Automatización');}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows}=await pool.query(`INSERT INTO proyectos (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/proyectos/:id', auth, async (req,res)=>{
  try {
    const {nombre,cliente_id,responsable,estatus}=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{sets.push(`${c}=$${i++}`);vals.push(v);};
    add('nombre',nombre);add('cliente_id',cliente_id);add('estatus',estatus);
    if(has('proyectos','responsable')) add('responsable',responsable);
    vals.push(req.params.id);
    const {rows}=await pool.query(`UPDATE proyectos SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.delete('/api/proyectos/:id', auth, adminOnly, async (req,res)=>{
  await Q('DELETE FROM proyectos WHERE id=$1',[req.params.id]); res.json({ok:true});
});

// ================================================================
// COTIZACIONES
// ================================================================
app.get('/api/cotizaciones', auth, async (req,res)=>{
  const dateCol=has('cotizaciones','created_at')?'c.created_at':'c.fecha_emision';
  res.json(await Q(`
    SELECT c.id, c.numero_cotizacion, c.fecha_emision, c.total, c.moneda, c.estatus,
           ${dateCol} fecha_orden,
           p.nombre proyecto_nombre, cl.nombre cliente_nombre, cl.email cliente_email
    FROM cotizaciones c
    LEFT JOIN proyectos p ON p.id=c.proyecto_id
    LEFT JOIN clientes cl ON cl.id=p.cliente_id
    ORDER BY ${dateCol} DESC`));
});

app.get('/api/cotizaciones/:id', auth, async (req,res)=>{
  const [c]=await Q(`
    SELECT c.*,
      p.nombre proyecto_nombre,
      cl.nombre cliente_nombre, cl.contacto cliente_contacto,
      cl.email cliente_email, cl.telefono cliente_tel,
      cl.direccion cliente_dir,
      CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='clientes' AND column_name='rfc') THEN cl.rfc ELSE NULL END cliente_rfc
    FROM cotizaciones c
    LEFT JOIN proyectos p ON p.id=c.proyecto_id
    LEFT JOIN clientes cl ON cl.id=p.cliente_id
    WHERE c.id=$1`,[req.params.id]);
  if(!c) return res.status(404).json({error:'No encontrada'});
  const items=await Q('SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[req.params.id]);
  const segs =await Q('SELECT * FROM seguimientos WHERE cotizacion_id=$1 ORDER BY fecha DESC',[req.params.id]);
  res.json({...c,items,seguimientos:segs});
});

app.post('/api/cotizaciones', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const {proyecto_id,moneda,items=[],folio,alcance_tecnico,notas_importantes,
           comentarios_generales,servicio_postventa,condiciones_entrega,condiciones_pago,
           garantia,responsabilidad,validez,fuerza_mayor,ley_aplicable,validez_hasta}=req.body;
    const yr=new Date().getFullYear();
    const cnt=(await client.query("SELECT COUNT(*) FROM cotizaciones WHERE fecha_emision::text LIKE $1",[`${yr}%`])).rows[0].count;
    const num=folio||`COT-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const total=items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0);

    // Construir INSERT dinámico para cotizaciones
    const cols=['proyecto_id','numero_cotizacion','total','moneda','estatus',
                'alcance_tecnico','notas_importantes','comentarios_generales',
                'condiciones_pago','garantia','validez_hasta'];
    const vals=[proyecto_id||null,num,total,moneda||'USD','borrador',
                alcance_tecnico,notas_importantes,comentarios_generales,
                condiciones_pago,garantia,validez_hasta||null];
    const opt=[
      ['servicio_postventa',servicio_postventa],['condiciones_entrega',condiciones_entrega],
      ['responsabilidad',responsabilidad],['validez',validez],
      ['fuerza_mayor',fuerza_mayor],['ley_aplicable',ley_aplicable],
    ];
    for(const [c,v] of opt){ if(has('cotizaciones',c)){cols.push(c);vals.push(v);} }
    if(has('cotizaciones','created_by')){cols.push('created_by');vals.push(req.user.id);}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows:[cot]}=await client.query(`INSERT INTO cotizaciones (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    for(const it of items){
      await client.query(
        'INSERT INTO items_cotizacion (cotizacion_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
        [cot.id,it.descripcion,it.cantidad,it.precio_unitario,(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
    }
    await client.query('COMMIT');
    res.status(201).json(cot);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.put('/api/cotizaciones/:id', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const {estatus,moneda,items,alcance_tecnico,notas_importantes,comentarios_generales,
           servicio_postventa,condiciones_entrega,condiciones_pago,garantia,
           responsabilidad,validez,fuerza_mayor,ley_aplicable,validez_hasta}=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(k,v)=>{if(v!==undefined){sets.push(`${k}=$${i++}`);vals.push(v);}};
    add('estatus',estatus);add('moneda',moneda);
    add('alcance_tecnico',alcance_tecnico);add('notas_importantes',notas_importantes);
    add('comentarios_generales',comentarios_generales);add('condiciones_pago',condiciones_pago);
    add('garantia',garantia);add('validez_hasta',validez_hasta);
    if(has('cotizaciones','servicio_postventa')) add('servicio_postventa',servicio_postventa);
    if(has('cotizaciones','condiciones_entrega')) add('condiciones_entrega',condiciones_entrega);
    if(has('cotizaciones','responsabilidad')) add('responsabilidad',responsabilidad);
    if(has('cotizaciones','validez')) add('validez',validez);
    if(has('cotizaciones','fuerza_mayor')) add('fuerza_mayor',fuerza_mayor);
    if(has('cotizaciones','ley_aplicable')) add('ley_aplicable',ley_aplicable);
    if(items){ const t=items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0); add('total',t); }
    if(sets.length){ vals.push(req.params.id); await client.query(`UPDATE cotizaciones SET ${sets.join(',')} WHERE id=$${i}`,vals); }
    if(items){
      await client.query('DELETE FROM items_cotizacion WHERE cotizacion_id=$1',[req.params.id]);
      for(const it of items) await client.query(
        'INSERT INTO items_cotizacion (cotizacion_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
        [req.params.id,it.descripcion,it.cantidad,it.precio_unitario,(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
    }
    await client.query('COMMIT');
    res.json((await Q('SELECT * FROM cotizaciones WHERE id=$1',[req.params.id]))[0]);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.delete('/api/cotizaciones/:id', auth, adminOnly, async (req,res)=>{
  try {
    await pool.query('DELETE FROM items_cotizacion WHERE cotizacion_id=$1',[req.params.id]);
    await pool.query('DELETE FROM seguimientos WHERE cotizacion_id=$1',[req.params.id]);
    await pool.query('DELETE FROM cotizaciones WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/cotizaciones/:id/seguimiento', auth, async (req,res)=>{
  try {
    const {tipo,notas,proxima_accion}=req.body;
    const {rows}=await pool.query(
      'INSERT INTO seguimientos (cotizacion_id,tipo,notas,proxima_accion) VALUES ($1,$2,$3,$4) RETURNING *',
      [req.params.id,tipo,notas,proxima_accion]);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/cotizaciones/:id/pdf', auth, async (req,res)=>{
  try {
    const [cot]=await Q(`
      SELECT c.*,p.nombre proyecto_nombre,
        cl.nombre cliente_nombre,cl.contacto cliente_contacto,
        cl.email cliente_email,cl.telefono cliente_tel,cl.direccion cliente_dir,
        COALESCE((SELECT rfc FROM clientes WHERE id=cl.id),NULL) cliente_rfc
      FROM cotizaciones c
      LEFT JOIN proyectos p ON p.id=c.proyecto_id
      LEFT JOIN clientes cl ON cl.id=p.cliente_id
      WHERE c.id=$1`,[req.params.id]);
    if(!cot) return res.status(404).json({error:'No encontrada'});
    const items=await Q('SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[req.params.id]);
    const buf=await buildPDFCotizacion(cot,items);
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="COT-${cot.numero_cotizacion}.pdf"`);
    res.send(buf);
  }catch(e){console.error(e);res.status(500).json({error:e.message});}
});

app.post('/api/cotizaciones/:id/email', auth, async (req,res)=>{
  try {
    const {to,cc,asunto,mensaje}=req.body;
    if(!to) return res.status(400).json({error:'to requerido'});
    const [cot]=await Q(`
      SELECT c.*,p.nombre proyecto_nombre,
        cl.nombre cliente_nombre,cl.contacto cliente_contacto,
        cl.email cliente_email,cl.telefono cliente_tel,cl.direccion cliente_dir
      FROM cotizaciones c
      LEFT JOIN proyectos p ON p.id=c.proyecto_id
      LEFT JOIN clientes cl ON cl.id=p.cliente_id
      WHERE c.id=$1`,[req.params.id]);
    if(!cot) return res.status(404).json({error:'No encontrada'});
    const items=await Q('SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[req.params.id]);
    const buf=await buildPDFCotizacion(cot,items);
    const sym=(cot.moneda||'USD')==='USD'?'$':'MX$';
    await mailer.sendMail({
      from:`"${VEF_NOMBRE}" <${process.env.SMTP_USER}>`,
      to,cc:cc||undefined,
      subject:asunto||`Cotización ${cot.numero_cotizacion} — ${VEF_NOMBRE}`,
      html:`<div style="font-family:Arial,sans-serif;max-width:600px">
        <div style="background:#0D2B55;padding:18px;text-align:center"><h2 style="color:#fff;margin:0">${VEF_NOMBRE}</h2><p style="color:#A8C5F0;margin:4px 0">${VEF_TELEFONO}</p></div>
        <div style="padding:20px"><p>${mensaje||'Estimado cliente, adjuntamos la cotización solicitada.'}</p>
        <p><b>Cotización:</b> ${cot.numero_cotizacion}<br><b>Total:</b> ${sym}${parseFloat(cot.total||0).toLocaleString('es-MX',{minimumFractionDigits:2})} ${cot.moneda||'USD'}</p></div>
        <div style="background:#0D2B55;padding:10px;text-align:center;color:#A8C5F0;font-size:12px">${VEF_NOMBRE} · ${VEF_TELEFONO} · ${VEF_CORREO}</div></div>`,
      attachments:[{filename:`COT-${cot.numero_cotizacion}.pdf`,content:buf}]
    });
    res.json({ok:true,msg:`Correo enviado a ${to}`});
  }catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// ORDENES DE PROVEEDOR
// ================================================================
app.get('/api/ordenes-proveedor', auth, async (req,res)=>{
  const dateCol=has('ordenes_proveedor','created_at')?'op.created_at':'op.fecha_emision';
  res.json(await Q(`
    SELECT op.*,p.nombre proveedor_nombre,p.email proveedor_email
    FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id
    ORDER BY ${dateCol} DESC`));
});

app.get('/api/ordenes-proveedor/:id', auth, async (req,res)=>{
  const [op]=await Q(`
    SELECT op.*,p.nombre proveedor_nombre,p.contacto proveedor_contacto,
           p.email proveedor_email,p.telefono proveedor_tel,
           p.direccion proveedor_dir,p.rfc proveedor_rfc
    FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id
    WHERE op.id=$1`,[req.params.id]);
  if(!op) return res.status(404).json({error:'No encontrada'});
  const items=await Q('SELECT * FROM items_orden_proveedor WHERE orden_id=$1 ORDER BY id',[req.params.id]);
  const segs =await Q('SELECT * FROM seguimientos_oc WHERE orden_id=$1 ORDER BY fecha DESC',[req.params.id]);
  res.json({...op,items,seguimientos:segs});
});

app.post('/api/ordenes-proveedor', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const {proveedor_id,moneda,items=[],condiciones_pago,fecha_entrega,lugar_entrega,notas,folio}=req.body;
    const yr=new Date().getFullYear();
    const cnt=(await client.query("SELECT COUNT(*) FROM ordenes_proveedor WHERE fecha_emision::text LIKE $1",[`${yr}%`])).rows[0].count;
    const num=folio||`OP-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const total=items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0);
    const {rows:[op]}=await client.query(
      `INSERT INTO ordenes_proveedor (proveedor_id,numero_op,moneda,total,condiciones_pago,fecha_entrega,lugar_entrega,notas,estatus)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'borrador') RETURNING *`,
      [proveedor_id,num,moneda||'USD',total,condiciones_pago,fecha_entrega||null,lugar_entrega,notas]);
    for(const it of items) await client.query(
      'INSERT INTO items_orden_proveedor (orden_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
      [op.id,it.descripcion,it.cantidad,it.precio_unitario,(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0)]);
    await client.query('COMMIT');
    res.status(201).json(op);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.put('/api/ordenes-proveedor/:id', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const {estatus,notas,proveedor_id,moneda,fecha_entrega,lugar_entrega,condiciones_pago,total,items}=req.body;
    // Build dynamic UPDATE
    const sets=[];const vals=[];let i=1;
    const add=(k,v)=>{if(v!==undefined){sets.push(`${k}=$${i++}`);vals.push(v);}};
    add('estatus',estatus);add('notas',notas);
    add('proveedor_id',proveedor_id?parseInt(proveedor_id):undefined);
    add('moneda',moneda);add('fecha_entrega',fecha_entrega||null);
    add('lugar_entrega',lugar_entrega);add('condiciones_pago',condiciones_pago);
    if(total!==undefined) add('total',total);
    if(sets.length){ vals.push(req.params.id); await client.query(`UPDATE ordenes_proveedor SET ${sets.join(',')} WHERE id=$${i}`,vals); }
    // Update items if provided
    if(items){
      await client.query('DELETE FROM items_orden_proveedor WHERE orden_id=$1',[req.params.id]);
      for(const it of items){
        const tot=(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0);
        await client.query('INSERT INTO items_orden_proveedor (orden_id,descripcion,cantidad,precio_unitario,total) VALUES ($1,$2,$3,$4,$5)',
          [req.params.id,it.descripcion,it.cantidad,it.precio_unitario,tot]);
      }
      const newTotal=items.reduce((s,it)=>s+(parseFloat(it.cantidad)||0)*(parseFloat(it.precio_unitario)||0),0);
      await client.query('UPDATE ordenes_proveedor SET total=$1 WHERE id=$2',[newTotal,req.params.id]);
    }
    await client.query('COMMIT');
    const [updated]=(await pool.query('SELECT * FROM ordenes_proveedor WHERE id=$1',[req.params.id])).rows;
    res.json(updated);
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.delete('/api/ordenes-proveedor/:id', auth, adminOnly, async (req,res)=>{
  try {
    await pool.query('DELETE FROM items_orden_proveedor WHERE orden_id=$1',[req.params.id]);
    await pool.query('DELETE FROM seguimientos_oc WHERE orden_id=$1',[req.params.id]);
    await pool.query('DELETE FROM ordenes_proveedor WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/ordenes-proveedor/:id/seguimiento', auth, async (req,res)=>{
  try {
    const {tipo,notas,proxima_accion}=req.body;
    const {rows}=await pool.query(
      'INSERT INTO seguimientos_oc (orden_id,tipo,notas,proxima_accion) VALUES ($1,$2,$3,$4) RETURNING *',
      [req.params.id,tipo,notas,proxima_accion]);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/ordenes-proveedor/:id/pdf', auth, async (req,res)=>{
  try {
    const [op]=await Q(`
      SELECT op.*,p.nombre proveedor_nombre,p.contacto proveedor_contacto,
             p.email proveedor_email,p.telefono proveedor_tel,
             p.direccion proveedor_dir,p.rfc proveedor_rfc
      FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id
      WHERE op.id=$1`,[req.params.id]);
    if(!op) return res.status(404).json({error:'No encontrada'});
    const items=await Q('SELECT * FROM items_orden_proveedor WHERE orden_id=$1 ORDER BY id',[req.params.id]);
    const buf=await buildPDFOrden(op,items);
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="OP-${op.numero_op}.pdf"`);
    res.send(buf);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/ordenes-proveedor/:id/email', auth, async (req,res)=>{
  try {
    const {to,cc,mensaje}=req.body;
    const [op]=await Q(`SELECT op.*,p.nombre proveedor_nombre,p.email proveedor_email,p.contacto proveedor_contacto,p.telefono proveedor_tel,p.direccion proveedor_dir,p.rfc proveedor_rfc FROM ordenes_proveedor op LEFT JOIN proveedores p ON p.id=op.proveedor_id WHERE op.id=$1`,[req.params.id]);
    if(!op) return res.status(404).json({error:'No encontrada'});
    const items=await Q('SELECT * FROM items_orden_proveedor WHERE orden_id=$1 ORDER BY id',[req.params.id]);
    const dest=to||op.proveedor_email;
    if(!dest) return res.status(400).json({error:'Destinatario requerido'});
    const buf=await buildPDFOrden(op,items);
    await mailer.sendMail({
      from:`"${VEF_NOMBRE}" <${process.env.SMTP_USER}>`,to:dest,cc:cc||undefined,
      subject:`Orden de Compra ${op.numero_op} — ${VEF_NOMBRE}`,
      html:`<p>${mensaje||'Estimado proveedor, adjuntamos la orden de compra.'}</p><p>OC: <b>${op.numero_op}</b></p>`,
      attachments:[{filename:`OP-${op.numero_op}.pdf`,content:buf}]
    });
    res.json({ok:true,msg:`Enviado a ${dest}`});
  }catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// FACTURAS
// ================================================================
app.get('/api/facturas', auth, async (req,res)=>{
  const filtro=req.query.estatus;
  const estCol=has('facturas','estatus_pago')?'f.estatus_pago':'f.estatus';
  const monedaCol=has('facturas','moneda')?"f.moneda":"'USD'";
  const totalCol=has('facturas','total')?'f.total':has('facturas','monto')?'f.monto':'0';
  let sql=`
    SELECT f.id, f.numero_factura, ${totalCol} total, ${monedaCol} moneda,
           ${estCol} estatus, f.fecha_emision,
           ${has('facturas','fecha_vencimiento')?'f.fecha_vencimiento,':''}
           COALESCE(c.numero_cotizacion,'—') numero_cotizacion,
           COALESCE(cl.nombre,'—') cliente_nombre,
           COALESCE((SELECT SUM(p.monto) FROM pagos p WHERE p.factura_id=f.id),0) pagado
    FROM facturas f
    LEFT JOIN cotizaciones c ON c.id=f.cotizacion_id
    LEFT JOIN clientes cl ON cl.id=${has('facturas','cliente_id')?'f.cliente_id':'c.proyecto_id'}
    WHERE 1=1`;
  const params=[];
  if(filtro&&filtro!=='todos'){
    if(filtro==='vencidas') sql+=` AND ${estCol}!='pagada' AND f.fecha_vencimiento<CURRENT_DATE`;
    else{ sql+=` AND ${estCol}=$1`; params.push(filtro); }
  }
  sql+=' ORDER BY f.id DESC';
  res.json(await Q(sql,params));
});

app.post('/api/facturas', auth, async (req,res)=>{
  try {
    const {cotizacion_id,cliente_id,moneda,subtotal,iva,total,fecha_vencimiento,notas}=req.body;
    const yr=new Date().getFullYear();
    const cnt=(await pool.query("SELECT COUNT(*) FROM facturas WHERE fecha_emision::text LIKE $1",[`${yr}%`])).rows[0].count;
    const num=`FAC-${yr}-${String(parseInt(cnt)+1).padStart(3,'0')}`;
    const cols=['numero_factura','cotizacion_id'];
    const vals=[num,cotizacion_id||null];
    const maybePush=(col,val)=>{ if(has('facturas',col)){cols.push(col);vals.push(val);} };
    maybePush('cliente_id',cliente_id||null);
    maybePush('moneda',moneda||'USD');
    maybePush('subtotal',subtotal||0);
    maybePush('iva',iva||0);
    maybePush('total',total||0);
    maybePush('monto',total||0);
    maybePush('fecha_vencimiento',fecha_vencimiento||null);
    maybePush('notas',notas);
    maybePush('estatus','pendiente');
    maybePush('estatus_pago','pendiente');
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows}=await pool.query(`INSERT INTO facturas (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.put('/api/facturas/:id', auth, async (req,res)=>{
  try {
    const {estatus,notas,fecha_vencimiento}=req.body;
    const sets=[];const vals=[];let i=1;
    if(has('facturas','estatus')){sets.push(`estatus=$${i++}`);vals.push(estatus);}
    if(has('facturas','estatus_pago')){sets.push(`estatus_pago=$${i++}`);vals.push(estatus);}
    if(notas!==undefined&&has('facturas','notas')){sets.push(`notas=$${i++}`);vals.push(notas);}
    if(fecha_vencimiento!==undefined&&has('facturas','fecha_vencimiento')){sets.push(`fecha_vencimiento=$${i++}`);vals.push(fecha_vencimiento||null);}
    if(!sets.length) return res.status(400).json({error:'Nada que actualizar'});
    vals.push(req.params.id);
    const {rows}=await pool.query(`UPDATE facturas SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/facturas/:id/pago', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const {monto,metodo,referencia,notas,fecha}=req.body;
    // Insert with fecha if column exists
    const fechaVal=fecha||null;
    if(has('pagos','fecha')){
      await client.query('INSERT INTO pagos (factura_id,monto,metodo,referencia,notas,fecha) VALUES ($1,$2,$3,$4,$5,$6)',
        [req.params.id,monto,metodo,referencia,notas,fechaVal]);
    } else {
      await client.query('INSERT INTO pagos (factura_id,monto,metodo,referencia,notas) VALUES ($1,$2,$3,$4,$5)',
        [req.params.id,monto,metodo,referencia,notas]);
    }
    const pg=(await client.query('SELECT COALESCE(SUM(monto),0) total FROM pagos WHERE factura_id=$1',[req.params.id])).rows[0];
    const ft=(await client.query(`SELECT COALESCE(total,monto,0) total FROM facturas WHERE id=$1`,[req.params.id])).rows[0];
    const pagado=parseFloat(pg.total), totalF=parseFloat(ft?.total||0);
    const estatus=pagado>=totalF?'pagada':pagado>0?'parcial':'pendiente';
    if(has('facturas','estatus')) await client.query('UPDATE facturas SET estatus=$1 WHERE id=$2',[estatus,req.params.id]);
    if(has('facturas','estatus_pago')) await client.query('UPDATE facturas SET estatus_pago=$1 WHERE id=$2',[estatus,req.params.id]);
    await client.query('COMMIT');
    res.json({ok:true,estatus,pagado,saldo:totalF-pagado});
  }catch(e){await client.query('ROLLBACK');res.status(500).json({error:e.message});}
  finally{client.release();}
});

app.get('/api/facturas/:id/pagos', auth, async (req,res)=>{
  res.json(await Q('SELECT * FROM pagos WHERE factura_id=$1 ORDER BY fecha DESC',[req.params.id]));
});

app.delete('/api/facturas/:id', auth, adminOnly, async (req,res)=>{
  try {
    await pool.query('DELETE FROM pagos WHERE factura_id=$1',[req.params.id]);
    await pool.query('DELETE FROM facturas WHERE id=$1',[req.params.id]);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/facturas/:id/pdf', auth, async (req,res)=>{
  try {
    const [f]=await Q(`
      SELECT f.*,cl.nombre cliente_nombre,cl.rfc cliente_rfc,
             cl.email cliente_email,cl.telefono cliente_tel
      FROM facturas f LEFT JOIN clientes cl ON cl.id=f.cliente_id
      WHERE f.id=$1`,[req.params.id]);
    if(!f) return res.status(404).json({error:'No encontrada'});
    const items=f.cotizacion_id?await Q('SELECT * FROM items_cotizacion WHERE cotizacion_id=$1 ORDER BY id',[f.cotizacion_id]):[];
    const buf=await buildPDFFactura(f,items);
    res.setHeader('Content-Type','application/pdf');
    res.setHeader('Content-Disposition',`inline; filename="FAC-${f.numero_factura}.pdf"`);
    res.send(buf);
  }catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// INVENTARIO
// ================================================================
app.get('/api/inventario', auth, async (req,res)=>{
  const actFil=has('inventario','activo')?'WHERE i.activo=true':'';
  const cantCol=has('inventario','cantidad_actual')?'i.cantidad_actual':has('inventario','stock_actual')?'i.stock_actual':'0';
  const minCol =has('inventario','cantidad_minima')?'i.cantidad_minima':has('inventario','stock_minimo')?'i.stock_minimo':'0';
  res.json(await Q(`
    SELECT i.*,${cantCol} qty_actual,${minCol} qty_minima,pr.nombre proveedor_nombre
    FROM inventario i LEFT JOIN proveedores pr ON pr.id=i.proveedor_id
    ${actFil} ORDER BY i.nombre`));
});

app.post('/api/inventario', auth, async (req,res)=>{
  try {
    const {codigo,nombre,descripcion,categoria,unidad,cantidad_actual,cantidad_minima,
           precio_costo,precio_venta,ubicacion,proveedor_id,notas}=req.body;
    const cols=['nombre','descripcion','categoria','unidad','precio_costo','precio_venta'];
    const vals=[nombre,descripcion,categoria,unidad||'pza',precio_costo||0,precio_venta||0];
    const mp=(c,v)=>{if(has('inventario',c)){cols.push(c);vals.push(v);}};
    mp('codigo',codigo);mp('ubicacion',ubicacion);mp('notas',notas);
    mp('proveedor_id',proveedor_id||null);
    mp('cantidad_actual',cantidad_actual||0);mp('cantidad_minima',cantidad_minima||0);
    mp('stock_actual',cantidad_actual||0);mp('stock_minimo',cantidad_minima||0);
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows}=await pool.query(`INSERT INTO inventario (${cols.join(',')}) VALUES (${ph}) RETURNING *`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.put('/api/inventario/:id', auth, async (req,res)=>{
  try {
    const {codigo,nombre,descripcion,categoria,unidad,cantidad_minima,precio_costo,precio_venta,ubicacion,notas}=req.body;
    const sets=[];const vals=[];let i=1;
    const add=(c,v)=>{if(has('inventario',c)||true){sets.push(`${c}=$${i++}`);vals.push(v);}};
    add('nombre',nombre);add('descripcion',descripcion);add('categoria',categoria);
    add('unidad',unidad);add('precio_costo',precio_costo||0);add('precio_venta',precio_venta||0);
    if(has('inventario','codigo')) {sets.push(`codigo=$${i++}`);vals.push(codigo);}
    if(has('inventario','ubicacion')){sets.push(`ubicacion=$${i++}`);vals.push(ubicacion);}
    if(has('inventario','notas')){sets.push(`notas=$${i++}`);vals.push(notas);}
    if(has('inventario','cantidad_minima')){sets.push(`cantidad_minima=$${i++}`);vals.push(cantidad_minima||0);}
    if(has('inventario','stock_minimo')){sets.push(`stock_minimo=$${i++}`);vals.push(cantidad_minima||0);}
    vals.push(req.params.id);
    const {rows}=await pool.query(`UPDATE inventario SET ${sets.join(',')} WHERE id=$${i} RETURNING *`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});

app.post('/api/inventario/:id/movimiento', auth, async (req,res)=>{
  const client=await pool.connect();
  try {
    await client.query('BEGIN');
    const {tipo,cantidad,notas,referencia}=req.body;
    const cant=parseFloat(cantidad)||0;
    const cantCol=has('inventario','cantidad_actual')?'cantidad_actual':has('inventario','stock_actual')?'stock_actual':'cantidad_actual';
    const [prod]=(await client.query(`SELECT COALESCE(${cantCol},0) stock FROM inventario WHERE id=$1`,[req.params.id])).rows;
    if(!prod) throw new Error('Producto no encontrado');
    let nuevo=parseFloat(prod.stock)||0;
    if(tipo==='entrada') nuevo+=cant;
    else if(tipo==='salida'){if(nuevo<cant) throw new Error('Stock insuficiente');nuevo-=cant;}
    else if(tipo==='ajuste') nuevo=cant;
    const upd=[];if(has('inventario','cantidad_actual')) upd.push(`cantidad_actual=${nuevo}`);
    if(has('inventario','stock_actual')) upd.push(`stock_actual=${nuevo}`);
    if(has('inventario','fecha_ultima_entrada')) upd.push(`fecha_ultima_entrada=CURRENT_DATE`);
    await client.query(`UPDATE inventario SET ${upd.join(',')} WHERE id=$1`,[req.params.id]);
    // Insertar movimiento — intenta con columnas extendidas, si falla usa mínimas
    try {
      const mCols=['producto_id','tipo','cantidad'];
      const mVals=[req.params.id,tipo,cant];
      const mAdd=(col,val)=>{ if(has('movimientos_inventario',col)){mCols.push(col);mVals.push(val);} };
      mAdd('stock_anterior',    prod.stock);
      mAdd('stock_nuevo',       nuevo);
      mAdd('cantidad_anterior', prod.stock);
      mAdd('cantidad_nueva',    nuevo);
      mAdd('notas',             notas||null);
      mAdd('referencia',        referencia||null);
      mAdd('created_by',        req.user.id);
      const mPh=mVals.map((_,i)=>`$${i+1}`).join(',');
      await client.query(`INSERT INTO movimientos_inventario (${mCols.join(',')}) VALUES (${mPh})`,mVals);
    } catch(e2) {
      // Fallback: solo columnas mínimas garantizadas
      console.warn('movimiento INSERT fallback:', e2.message);
      await client.query(
        'INSERT INTO movimientos_inventario (producto_id,tipo,cantidad) VALUES ($1,$2,$3)',
        [req.params.id,tipo,cant]);
    }
    await client.query('COMMIT');
    res.json({ok:true,stock_nuevo:nuevo});
  }catch(e){await client.query('ROLLBACK');res.status(400).json({error:e.message});}
  finally{client.release();}
});

app.get('/api/inventario/movimientos', auth, async (req,res)=>{
  res.json(await Q(`
    SELECT m.*,i.nombre producto_nombre
    FROM movimientos_inventario m LEFT JOIN inventario i ON i.id=m.producto_id
    ORDER BY m.fecha DESC LIMIT 200`));
});

app.delete('/api/inventario/:id', auth, adminOnly, async (req,res)=>{
  if(has('inventario','activo')) await Q('UPDATE inventario SET activo=false WHERE id=$1',[req.params.id]);
  else await Q('DELETE FROM inventario WHERE id=$1',[req.params.id]);
  res.json({ok:true});
});

// ================================================================
// USUARIOS
// ================================================================
app.get('/api/usuarios', auth, adminOnly, async (req,res)=>{
  const emailCol=has('usuarios','email')?'email,':'';
  res.json(await Q(`SELECT id,username,nombre,${emailCol}rol,activo,ultimo_acceso FROM usuarios ORDER BY nombre`));
});
app.post('/api/usuarios', auth, adminOnly, async (req,res)=>{
  try {
    const {username,nombre,email,password,rol}=req.body;
    if(!username||!password) return res.status(400).json({error:'username y password requeridos'});
    const hash=await bcrypt.hash(password,12);
    // Insertar en columnas que existen — la BD puede tener 'password' o 'password_hash' o ambas
    const cols=['username','nombre','rol'];
    const vals=[username,nombre||username,rol||'usuario'];
    // Siempre llenar password_hash si existe
    if(has('usuarios','password_hash')){cols.push('password_hash');vals.push(hash);}
    // También llenar 'password' si existe (columna heredada del sistema anterior)
    if(has('usuarios','password')){cols.push('password');vals.push(hash);}
    // También 'contrasena' por si acaso
    if(has('usuarios','contrasena')){cols.push('contrasena');vals.push(hash);}
    if(has('usuarios','email')&&email){cols.push('email');vals.push(email);}
    if(has('usuarios','activo')){cols.push('activo');vals.push(true);}
    const ph=vals.map((_,i)=>`$${i+1}`).join(',');
    const {rows}=await pool.query(`INSERT INTO usuarios (${cols.join(',')}) VALUES (${ph}) RETURNING id,username,nombre,rol`,vals);
    res.status(201).json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.put('/api/usuarios/:id', auth, adminOnly, async (req,res)=>{
  try {
    const {nombre,email,rol,activo}=req.body;
    const sets=[];const vals=[];let i=1;
    sets.push(`nombre=$${i++}`);vals.push(nombre);
    sets.push(`rol=$${i++}`);vals.push(rol);
    if(has('usuarios','email')){sets.push(`email=$${i++}`);vals.push(email);}
    if(has('usuarios','activo')){sets.push(`activo=$${i++}`);vals.push(activo!==undefined?activo:true);}
    vals.push(req.params.id);
    const {rows}=await pool.query(`UPDATE usuarios SET ${sets.join(',')} WHERE id=$${i} RETURNING id,username,nombre,rol,activo`,vals);
    res.json(rows[0]);
  }catch(e){res.status(500).json({error:e.message});}
});
app.post('/api/usuarios/:id/reset-password', auth, adminOnly, async (req,res)=>{
  try {
    const {password}=req.body;
    if(!password) return res.status(400).json({error:'Nueva contraseña requerida'});
    const hash=await bcrypt.hash(password,12);
    // Actualizar todas las columnas de contraseña que existan
    const sets=[];const pvals=[];let pi=1;
    if(has('usuarios','password_hash')){sets.push(`password_hash=$${pi++}`);pvals.push(hash);}
    if(has('usuarios','password')){sets.push(`password=$${pi++}`);pvals.push(hash);}
    if(has('usuarios','contrasena')){sets.push(`contrasena=$${pi++}`);pvals.push(hash);}
    if(!sets.length) return res.status(500).json({error:'No se encontró columna de contraseña'});
    pvals.push(req.params.id);
    await pool.query(`UPDATE usuarios SET ${sets.join(',')} WHERE id=$${pi}`,pvals);
    res.json({ok:true});
  }catch(e){res.status(500).json({error:e.message});}
});

// ================================================================
// LOGO
// ================================================================
app.get('/api/logo/status', auth, (req,res)=>{
  const lp=getLogoPath();
  res.json({found:!!lp, filename:lp?path.basename(lp):null});
});

// ================================================================
// LOGO UPLOAD (base64) — guarda como logo.png en raíz del proyecto
// ================================================================
app.post('/api/logo/upload', auth, adminOnly, async (req,res)=>{
  try {
    const { data, mime, ext } = req.body;
    if (!data) return res.status(400).json({ error: 'data requerido' });
    const allowed = ['png','jpg','jpeg'];
    const extension = (ext||'png').toLowerCase().replace('jpeg','jpg');
    if (!allowed.includes(extension)) return res.status(400).json({ error: 'Solo PNG o JPG' });
    const buf = Buffer.from(data, 'base64');
    if (buf.length > 3 * 1024 * 1024) return res.status(400).json({ error: 'Archivo muy grande (máx 3MB)' });
    // Eliminar logos anteriores
    for (const n of ['logo.png','logo.jpg','logo.jpeg','logo.PNG','logo.JPG']) {
      const p = path.join(__dirname, n);
      if (fs.existsSync(p)) try { fs.unlinkSync(p); } catch {}
    }
    const dest = path.join(__dirname, 'logo.png');
    fs.writeFileSync(dest, buf);
    // Actualizar LOGO_PATH en memoria (para PDFs inmediatos)
    global._logoPathOverride = dest;
    console.log('🖼  Logo subido:', dest, buf.length, 'bytes');
    res.json({ ok: true, path: dest, size: buf.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ================================================================
// EMPRESA CONFIG — GET y PUT (upsert)
// ================================================================
app.get('/api/empresa', auth, async (req,res)=>{
  try {
    const r = await pool.query('SELECT * FROM empresa_config ORDER BY id LIMIT 1');
    res.json(r.rows[0] || {});
  } catch(e){ res.status(500).json({error:e.message}); }
});

app.put('/api/empresa', auth, adminOnly, async (req,res)=>{
  try {
    const {nombre,razon_social,rfc,regimen_fiscal,contacto,telefono,email,
           direccion,ciudad,estado,cp,pais,sitio_web,
           moneda_default,iva_default,notas_factura,notas_cotizacion} = req.body;
    // Verificar si ya existe
    const ex = await pool.query('SELECT id FROM empresa_config LIMIT 1');
    if (ex.rows.length > 0) {
      await pool.query(`UPDATE empresa_config SET
        nombre=$1, razon_social=$2, rfc=$3, regimen_fiscal=$4, contacto=$5,
        telefono=$6, email=$7, direccion=$8, ciudad=$9, estado=$10, cp=$11,
        pais=$12, sitio_web=$13, moneda_default=$14, iva_default=$15,
        notas_factura=$16, notas_cotizacion=$17, updated_at=NOW()
        WHERE id=$18`,
        [nombre,razon_social,rfc,regimen_fiscal,contacto,telefono,email,
         direccion,ciudad,estado,cp,pais||'México',sitio_web,
         moneda_default||'USD',iva_default||16,notas_factura,notas_cotizacion,
         ex.rows[0].id]);
    } else {
      await pool.query(`INSERT INTO empresa_config
        (nombre,razon_social,rfc,regimen_fiscal,contacto,telefono,email,
         direccion,ciudad,estado,cp,pais,sitio_web,moneda_default,iva_default,notas_factura,notas_cotizacion)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
        [nombre,razon_social,rfc,regimen_fiscal,contacto,telefono,email,
         direccion,ciudad,estado,cp,pais||'México',sitio_web,
         moneda_default||'USD',iva_default||16,notas_factura,notas_cotizacion]);
    }
    const updated = await pool.query('SELECT * FROM empresa_config ORDER BY id LIMIT 1');
    res.json(updated.rows[0]);
  } catch(e){ res.status(500).json({error:e.message}); }
});

// ================================================================
// EMAIL TEST
// ================================================================
app.post('/api/email/test', auth, async (req,res)=>{
  const { to } = req.body;
  if (!to) return res.status(400).json({ error: 'to requerido' });
  try {
    await mailer.sendMail({
      from: `"${VEF_NOMBRE}" <${process.env.SMTP_USER}>`,
      to,
      subject: `✅ Prueba de correo — ${VEF_NOMBRE}`,
      html: `<div style="font-family:Arial,sans-serif;max-width:500px">
        <div style="background:#0D2B55;padding:16px;text-align:center">
          <h2 style="color:#fff;margin:0">${VEF_NOMBRE}</h2>
          <p style="color:#A8C5F0;margin:4px 0">Prueba de configuración SMTP</p>
        </div>
        <div style="padding:20px">
          <p>✅ El correo está correctamente configurado.</p>
          <p><b>Servidor:</b> smtp.zoho.com · Puerto 465 (SSL)<br>
          <b>Cuenta:</b> ${process.env.SMTP_USER}<br>
          <b>Fecha:</b> ${new Date().toLocaleString('es-MX')}</p>
        </div>
        <div style="background:#0D2B55;padding:10px;text-align:center;color:#A8C5F0;font-size:12px">
          ${VEF_NOMBRE} · ${VEF_TELEFONO} · ${VEF_CORREO}
        </div>
      </div>`
    });
    res.json({ ok: true, msg: `Correo enviado a ${to}` });
  } catch(e) {
    console.error('Email test error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ================================================================
// LOGO PÚBLICO — sin auth, para mostrarlo en el HTML
// ================================================================
app.get('/logo.png', (req,res)=>{
  const lp = getLogoPath();
  if (!lp) return res.status(404).send('No logo');
  res.sendFile(lp);
});
app.get('/logo.jpg', (req,res)=>{
  const lp = getLogoPath();
  if (!lp) return res.status(404).send('No logo');
  res.sendFile(lp);
});

// ================================================================
// FRONTEND
// ================================================================
app.get('/app', (req,res)=>res.sendFile(path.join(__dirname,'frontend','app.html')));
app.get('*',   (req,res)=>res.sendFile(path.join(__dirname,'frontend','index.html')));

// ================================================================
// START
// ================================================================
app.listen(PORT, async ()=>{
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  VEF ERP — Puerto ${PORT}`);
  console.log(`  DB: ${process.env.DB_HOST}`);
  console.log('═'.repeat(50)+'\n');
  await autoSetup();
  console.log(`\n🚀 http://localhost:${PORT}\n`);
});
module.exports=app;
