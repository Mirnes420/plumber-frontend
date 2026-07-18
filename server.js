import express from 'express';
import makeWASocket, { useMultiFileAuthState, DisconnectReason, BufferJSON, initAuthCreds } from '@whiskeysockets/baileys';
import pino from 'pino';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import pg from 'pg';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import cookieParser from 'cookie-parser';

// ── Load env FIRST before reading any process.env values ──
dotenv.config({ path: '../.env' });

const { Pool } = pg;
const JWT_SECRET = process.env.ADMIN_JWT_SECRET || 'coherzo_admin_dev_secret_change_in_prod';

// Global cached connection pool to prevent Supabase connection exhaustion (EMAXCONNSESSION)
let dbPool = null;

function getDbPool() {
    if (!dbPool && process.env.DATABASE_URL) {
        console.log("🗄️ Initializing Global PostgreSQL Pool...");
        dbPool = new Pool({
            connectionString: process.env.DATABASE_URL,
            ssl: process.env.DATABASE_URL.includes('localhost') || process.env.DATABASE_URL.includes('127.0.0.1') ? false : { rejectUnauthorized: false },
            max: 3,             // extremely conservative connection limit
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 30000
        });
    }
    return dbPool;
}

// ESM equivalent for __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({ storage: multer.memoryStorage() });

// ── Admin DB Migration: ensure password_hash column exists ──
async function runAdminMigration() {
    const pool = getDbPool();
    if (!pool) return;
    try {
        await pool.query(`ALTER TABLE plumbers ADD COLUMN IF NOT EXISTS password_hash TEXT`);
        console.log('✅ Admin migration: password_hash column ready.');
    } catch (err) {
        console.error('⚠️ Admin migration error:', err.message);
    }
}

let currentQR = "";
let sock = null;
let isConnected = false;
let pushName = "";

const logger = pino({ level: 'silent' });

async function getAuthStateStore() {
    if (!process.env.DATABASE_URL) {
        console.log("⚠️ DATABASE_URL not set in .env. Falling back to local MultiFileAuth...");
        return useMultiFileAuthState(process.env.DATA_PATH || './.baileys_auth');
    }

    try {
        const pool = getDbPool();

        // Ensure table exists
        await pool.query(`
            CREATE TABLE IF NOT EXISTS whatsapp_auth_store (
                key VARCHAR(255) PRIMARY KEY,
                value TEXT
            )
        `);

        const readData = async (key) => {
            try {
                const res = await pool.query('SELECT value FROM whatsapp_auth_store WHERE key = $1', [key]);
                if (res.rows.length > 0) {
                    return JSON.parse(res.rows[0].value, BufferJSON.reviver);
                }
            } catch (err) {
                console.error(`DB read error for key ${key}:`, err.message);
            }
            return null;
        };

        const writeData = async (key, value) => {
            try {
                const serialized = JSON.stringify(value, BufferJSON.replacer);
                await pool.query(`
                    INSERT INTO whatsapp_auth_store (key, value)
                    VALUES ($1, $2)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                `, [key, serialized]);
            } catch (err) {
                console.error(`DB write error for key ${key}:`, err.message);
            }
        };

        const deleteData = async (key) => {
            try {
                await pool.query('DELETE FROM whatsapp_auth_store WHERE key = $1', [key]);
            } catch (err) {
                console.error(`DB delete error for key ${key}:`, err.message);
            }
        };

        let creds = await readData('creds');
        if (!creds) {
            creds = initAuthCreds();
            await writeData('creds', creds);
        }

        return {
            state: {
                creds,
                keys: {
                    get: async (type, ids) => {
                        const data = {};
                        await Promise.all(
                            ids.map(async (id) => {
                                let value = await readData(`${type}-${id}`);
                                if (value) {
                                    data[id] = value;
                                }
                            })
                        );
                        return data;
                    },
                    set: async (data) => {
                        const tasks = [];
                        for (const category in data) {
                            for (const id in data[category]) {
                                const value = data[category][id];
                                const key = `${category}-${id}`;
                                if (value) {
                                    tasks.push(writeData(key, value));
                                } else {
                                    tasks.push(deleteData(key));
                                }
                            }
                        }
                        await Promise.all(tasks);
                    }
                }
            },
            saveCreds: async () => {
                await writeData('creds', creds);
            }
        };

    } catch (err) {
        console.error("❌ Postgres Auth Store failed to initialize:", err.message);
        console.log("Falling back to local MultiFileAuth...");
        return useMultiFileAuthState(process.env.DATA_PATH || './.baileys_auth');
    }
}

async function startSock() {
    const { state, saveCreds } = await getAuthStateStore();

    sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: logger
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            currentQR = qr;
        }
        if (connection === 'close') {
            isConnected = false;
            pushName = "";
            const shouldReconnect = (lastDisconnect.error)?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log('Connection closed due to ', lastDisconnect.error, ', reconnecting: ', shouldReconnect);
            if (shouldReconnect) {
                setTimeout(startSock, 5000); // Reconnect after 5 seconds
            }
        } else if (connection === 'open') {
            console.log('✅ Baileys: WhatsApp connection opened successfully!');
            isConnected = true;
            currentQR = "";
            pushName = sock.user.name || "Admin/Plumber";
        }
    });

    sock.ev.on('messages.upsert', async (m) => {
        if (m.type !== 'notify') return;
        for (const msg of m.messages) {
            if (!msg.message) continue;

            const from = msg.key.remoteJid;
            const fromMe = msg.key.fromMe;

            // Get text content safely
            const body = msg.message.conversation ||
                msg.message.extendedTextMessage?.text ||
                msg.message.imageMessage?.caption || "";

            if (!body) continue;

            if (fromMe) {
                const bodyUpper = body.trim().toUpperCase();
                const commands = ["URGENT", "NOT URGENT", "ALL TASKS", "EMERGENCY", "NON EMERGENCY", "NO EMERGENCY", "FILTER", "MID", "ALL"];
                if (!commands.includes(bodyUpper)) {
                    continue;
                }
            }

            console.log(`Received message from ${from}: ${body}`);

            // Standardize remoteJid for python backend
            const cleanFrom = from.replace("@s.whatsapp.net", "").replace("@g.us", "").replace(/[^0-9]/g, "");

            const payload = {
                From: cleanFrom,
                Body: body
            };

            if (msg.message.imageMessage) {
                payload.MediaUrl0 = "media_attached_but_unsupported_by_simple_forwarder";
            }

            try {
                console.log('calling the webhook endpoint from the fastapi');
                await fetch(`${FASTAPI_URL}/webhook`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
            } catch (error) {
                console.error(`Error forwarding to webhook. Is FastAPI running on ${FASTAPI_URL}?`, error.message);
            }
        }
    });
}

startSock();
runAdminMigration();

// Serve the demo page when the URL explicitly requests demo mode.
app.get('/demo', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'demo.html'));
});

app.get('/demo.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'demo.html'));
});

// Route to serve index.html for specific plumber IDs (e.g. /id=1)
app.get('/id=:plumber_id', (req, res) => {
    const demoRequested = req.query.demo === 'true' || req.query.demo === '1';
    if (demoRequested) {
        return res.sendFile(path.join(__dirname, 'public', 'demo.html'));
    }
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Support URLs where demo is embedded in the raw path (for example: /id=1&demo==true)
app.get(/^\/id=.*$/i, (req, res) => {
    const rawUrl = req.originalUrl || '';
    const demoRequested = /(?:^|[?&])demo(?:=|==)?(?:true|1)/i.test(rawUrl);

    if (demoRequested) {
        return res.sendFile(path.join(__dirname, 'public', 'demo.html'));
    }

    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Safe API endpoint to get the current QR code data (protected with password)
app.get('/qr', (req, res) => {
    if (req.query.pwd !== 'Djemenadje#1') {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    if (isConnected && pushName) {
        return res.json({ status: 'connected', name: pushName });
    }
    if (!currentQR) {
        return res.json({ status: 'pending' });
    }
    res.json({ status: 'qr', qr: currentQR });
});

// Web interface to scan the QR Code from the cloud!
app.get('/auth', (req, res) => {
    if (req.query.pwd !== 'Djemenadje#1') {
        return res.status(401).send(`
            <html>
                <head>
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Unauthorized</title>
                    <style>
                        body { display:flex; justify-content:center; align-items:center; height:100vh; background:#050505; margin:0; font-family:sans-serif; color:white; }
                        .card { text-align:center; padding: 2.5rem; background:#121214; border-radius:20px; border:1px solid #27272a; max-width:400px; width:90%; box-shadow: 0 20px 40px rgba(0,0,0,0.8); }
                    </style>
                </head>
                <body>
                    <div class="card">
                        <h2 style="color:#f43f5e; margin:0 0 10px 0;">Access Denied</h2>
                        <p style="color:#a1a1aa; font-size:14px; line-height:1.6; margin:0;">This administrative portal is password protected.<br><br>Please provide the password in the URL query string, e.g.:<br><code style="color:white; background:#1e1e20; padding:4px 8px; border-radius:4px;">/auth?pwd=Djemenadje#1</code></p>
                    </div>
                </body>
            </html>
        `);
    }

    res.send(`
        <html>
            <head>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>WhatsApp Bot Auth</title>
                <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
                <style>
                    body { display:flex; justify-content:center; align-items:center; height:100vh; background:#050505; margin:0; font-family:sans-serif; color:white; }
                    .card { text-align:center; padding: 2.5rem; background:#121214; border-radius:20px; border:1px solid #27272a; max-width:400px; width:90%; box-shadow: 0 20px 40px rgba(0,0,0,0.8); }
                    #qrcode { margin: 20px auto; background: white; padding: 12px; border-radius: 12px; display:inline-block; }
                    #status { color:#a1a1aa; font-size:14px; margin-top:10px; line-height: 1.4; }
                </style>
            </head>
            <body>
                <!-- QR Scanner Card -->
                <div class="card" id="qr-card">
                    <h2 style="margin: 0 0 10px 0;">Link your WhatsApp</h2>
                    <p style="color: #a1a1aa; font-size: 14px; margin: 0 0 20px 0;">Scan this QR code with your WhatsApp app.</p>
                    <div id="qrcode"></div>
                    <p id="status">Loading...</p>
                </div>

                <script>
                    let currentQRText = '';
                    let pollTimer = null;
                    
                    // Automatically capture the password from URL query string
                    function getPassword() {
                        const urlParams = new URLSearchParams(window.location.search);
                        return urlParams.get('pwd') || '';
                    }

                    async function checkStatus() {
                        const pwd = getPassword();
                        if (!pwd) {
                            document.getElementById('status').textContent = 'Error: Missing password parameter in URL.';
                            return;
                        }
                        try {
                            const res = await fetch('/qr?pwd=' + encodeURIComponent(pwd));
                            if (res.status === 401) {
                                document.getElementById('status').textContent = 'Unauthorized: Invalid password parameter.';
                                return;
                            }
                            const data = await res.json();
                            const statusEl = document.getElementById('status');
                            const qrEl = document.getElementById('qrcode');
                            
                            if (data.status === 'connected') {
                                statusEl.style.color = '#09f195';
                                statusEl.textContent = 'WhatsApp connected as ' + data.name + '!';
                                qrEl.innerHTML = '<div style="font-size:64px">✅</div>';
                                if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
                            } else if (data.status === 'qr') {
                                if (data.qr !== currentQRText) {
                                    currentQRText = data.qr;
                                    qrEl.innerHTML = '';
                                    new QRCode(qrEl, { text: data.qr, width: 256, height: 256 });
                                }
                                statusEl.textContent = 'Scan now! QR updates automatically.';
                            } else {
                                statusEl.textContent = 'Generating QR code...';
                            }
                        } catch(e) {
                            document.getElementById('status').textContent = 'Error: ' + e.message;
                        }
                    }

                    // Initialize state
                    checkStatus();
                    pollTimer = setInterval(checkStatus, 5000);
                </script>
            </body>
        </html>
    `);
});

// API Endpoint to send messages
app.post('/send', async (req, res) => {
    try {
        const { number, text, imageUrl, caption, buttons, demo } = req.body;


        if (!number) {
            return res.status(400).json({ error: 'Number is required (e.g. +385919293138 or "me")' });
        }

        if (!isConnected || !sock) {
            return res.status(503).json({ error: 'WhatsApp client is not connected' });
        }

        let chatId = number;
        if (number.toLowerCase() === "me") {
            if (!sock.user || !sock.user.id) {
                return res.status(500).json({ error: 'Cannot send to "me" because WhatsApp is not logged in yet.' });
            }
            chatId = sock.user.id.split(':')[0] + "@s.whatsapp.net";
        } else {
            const cleanNumber = number.replace(/[^0-9]/g, "");
            chatId = `${cleanNumber}@s.whatsapp.net`;
        }

        console.log(`Attempting to send message to ${chatId}`);

        if (buttons && buttons.length > 0) {
            console.log(`Sending keyword-optimized text menu to ${chatId}`);
            const menuHeader = text || "⚠️ *Action Required* ⚠️\nPlease select an option by replying with one of the keywords below:";
            const menuBody = buttons.map(b => `👉 *${b.toUpperCase().trim()}*`).join('\n');
            const fullMenuText = `${menuHeader}\n\n${menuBody}\n\n_Type your chosen keyword exactly as shown to respond._`;

            await sock.sendMessage(chatId, { text: fullMenuText });
            console.log("✅ Keyword text menu sent successfully");
        } else if (imageUrl) {
            console.log(`Sending image to ${chatId}`);
            let imageSource;
            if (imageUrl.startsWith('data:image') || !imageUrl.startsWith('http')) {
                // Extract raw base64 string
                const base64Data = imageUrl.replace(/^data:image\/[a-z]+;base64,/, "");
                imageSource = Buffer.from(base64Data, 'base64');
            } else {
                imageSource = { url: imageUrl };
            }
            await sock.sendMessage(chatId, {
                image: imageSource,
                caption: caption || text || ""
            });
            console.log("✅ Image sent successfully");
        } else if (text) {
            console.log(`Sending text to ${chatId}`);
            await sock.sendMessage(chatId, { text: text });
            console.log("✅ Text sent successfully");
        } else {
            return res.status(400).json({ error: 'Either text, imageUrl, or buttons is required' });
        }

        res.json({ success: true, message: 'Message sent!' });
    } catch (error) {
        console.error("Error sending message:", error);
        res.status(500).json({ error: error.message });
    }
});

// Endpoint to receive HTML form submissions and forward to FastAPI
app.post('/submit-form', upload.single('image'), async (req, res) => {
    try {
        const { phone, description, location, customer_name, plumber_id, demo, professional_type } = req.body;
        console.log(`🌐 Received web form from ${customer_name || 'Unknown'} (${phone}) [Plumber ID: ${plumber_id || 'None'} | Type: ${professional_type || 'plumber'} | Demo: ${demo}]`);

        // Handle Quick Demo Mode Bypass directly inside Express
        if (demo === 'true' || demo === true) {
            console.log(`🚀 Demo Mode Active: Intercepting request and mock-paging provider directly via WhatsApp`);
            
            if (!isConnected || !sock) {
                return res.status(503).json({ error: 'WhatsApp client is not connected' });
            }

            const cleanNumber = phone.replace(/[^0-9]/g, "");
            const chatId = `${cleanNumber}@s.whatsapp.net`;
            const typeUpper = (professional_type || 'plumber').toUpperCase();

            // Constructing a simulated real-world AI payload structure
            const mockAlertText = `🚨 *NEW EMERGENCY DISPATCH* 🚨\n\n` +
                                  `👤 *Customer:* ${customer_name || 'John Doe'}\n` +
                                  `📍 *Location:* ${location || '123 Main Street, Unit 4B'}\n` +
                                  `🛠️ *Trade Required:* ${typeUpper}\n\n` +
                                  `📋 *AI Incident Diagnosis:* ${description || 'System failure needing immediate dispatch.'}\n\n` +
                                  `⚡ *Action Required:* Please reply to this message immediately to confirm availability.`;

            await sock.sendMessage(chatId, { text: mockAlertText });
            console.log(`✅ Demo dispatch message successfully pushed to mock provider: ${chatId}`);
            
            return res.json({ success: true, demo: true, message: 'Demo request simulated successfully!' });
        }

        // ─── Normal Non-Demo Path continues here ───────────────────────────
        const formData = new FormData();
        formData.append('phone', phone);
        formData.append('description', description);
        
        if (location) formData.append('location', location);
        if (customer_name) formData.append('customer_name', customer_name);
        if (plumber_id) formData.append('plumber_id', plumber_id);
        if (demo) formData.append('demo', demo);
        if (professional_type) formData.append('professional_type', professional_type);

        if (req.file) {
            const blob = new Blob([req.file.buffer], { type: req.file.mimetype });
            formData.append('image', blob, req.file.originalname);
        }

        let result = null;
        let lastError = null;
        const maxRetries = 3;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                console.log(`Attempt ${attempt}/${maxRetries}: calling FastAPI /api/incident`);
                const response = await fetch(`${FASTAPI_URL}/api/incident`, {
                    method: 'POST',
                    body: formData
                });

                const contentType = response.headers.get("content-type");

                if (contentType && contentType.includes("application/json")) {
                    result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.detail ? JSON.stringify(result.detail) : 'FastAPI rejected the request');
                    }
                    break;
                } else {
                    await response.text();
                    if (attempt < maxRetries) await new Promise(r => setTimeout(r, 5000));
                }
            } catch (fetchErr) {
                lastError = fetchErr.message;
                if (attempt < maxRetries) await new Promise(r => setTimeout(r, 5000));
            }
        }

        if (!result) throw new Error(lastError || 'FastAPI server did not respond.');

        res.json({ success: true, result });
    } catch (error) {
        console.error("Web form processing error:", error);
        res.status(500).json({ error: error.message });
    }
});


// ==========================================================================
// ADMIN DASHBOARD PROXY ROUTES (Forward to FastAPI Backend)
// ==========================================================================

// Serve admin.html locally
app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// Proxy handler to Python FastAPI backend
async function proxyToFastAPI(req, res, targetPath) {
    const url = `${FASTAPI_URL}${targetPath}`;

    // Forward request options, mapping cookies or authorization headers
    const headers = {
        'Content-Type': 'application/json',
        'Authorization': req.headers['authorization'] || ''
    };

    if (req.headers.cookie) {
        headers['Cookie'] = req.headers.cookie;
    } else if (req.cookies?.admin_token) {
        headers['Cookie'] = `admin_token=${req.cookies.admin_token}`;
    }

    try {
        const fetchOptions = {
            method: req.method,
            headers: headers
        };

        if (['POST', 'PUT', 'PATCH'].includes(req.method) && req.body) {
            fetchOptions.body = JSON.stringify(req.body);
        }

        const response = await fetch(url, fetchOptions);
        const contentType = response.headers.get('content-type') || '';
        const setCookieHeader = response.headers.get('set-cookie');

        if (setCookieHeader) {
            const setCookieValues = setCookieHeader.split(/,(?=[^,]+?=)/).map(v => v.trim());
            for (const cookie of setCookieValues) {
                res.append('Set-Cookie', cookie);
            }
        }

        if (contentType.includes('application/json')) {
            const data = await response.json();
            res.status(response.status).json(data);
            return;
        }

        const text = await response.text();
        res.status(response.status).type(contentType || 'text/plain').send(text);
    } catch (err) {
        console.error(`Proxy error connecting to FastAPI (${url}):`, err.message);
        res.status(502).json({ error: 'Backend server communication failure.' });
    }
}

// Backward-compatible aliases for the admin UI's current fetch paths.
app.post('/admin/set-password', (req, res) => proxyToFastAPI(req, res, '/admin/set-password'));
app.post('/admin/login', (req, res) => proxyToFastAPI(req, res, '/admin/login'));
app.post('/admin/logout', (req, res) => {
    res.clearCookie('admin_token');
    res.json({ success: true });
});
app.get('/admin/me', (req, res) => proxyToFastAPI(req, res, '/admin/me'));
app.get('/admin/plumbers', (req, res) => proxyToFastAPI(req, res, '/admin/plumbers'));
app.get('/admin/incidents', (req, res) => {
    const query = new URLSearchParams(req.query).toString();
    proxyToFastAPI(req, res, `/admin/incidents?${query}`);
});
app.patch('/admin/incident-status', (req, res) => proxyToFastAPI(req, res, '/admin/incident-status'));

// Preferred /api/admin routes.
app.post('/api/admin/set-password', (req, res) => proxyToFastAPI(req, res, '/admin/set-password'));
app.post('/api/admin/login', (req, res) => proxyToFastAPI(req, res, '/admin/login'));
app.post('/api/admin/logout', (req, res) => {
    res.clearCookie('admin_token');
    res.json({ success: true });
});
app.get('/api/admin/me', (req, res) => proxyToFastAPI(req, res, '/admin/me'));
app.get('/api/admin/plumbers', (req, res) => proxyToFastAPI(req, res, '/admin/plumbers'));

app.get('/api/admin/incidents', (req, res) => {
    const query = new URLSearchParams(req.query).toString();
    proxyToFastAPI(req, res, `/admin/incidents?${query}`);
});

app.patch('/api/admin/incident-status', (req, res) => proxyToFastAPI(req, res, '/admin/incident-status'));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`wbot API server running on port ${PORT}`);
});