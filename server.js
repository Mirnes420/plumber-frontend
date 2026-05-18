import express from 'express';
import makeWASocket, { useMultiFileAuthState, DisconnectReason } from '@whiskeysockets/baileys';
import pino from 'pino';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Load shared environment variables
dotenv.config({ path: '../.env' });

// ESM equivalent for __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const FASTAPI_URL = process.env.FASTAPI_URL || 'http://localhost:8000';

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({ storage: multer.memoryStorage() });

let currentQR = "";
let sock = null;
let isConnected = false;
let pushName = "";

const logger = pino({ level: 'silent' });

async function startSock() {
    const { state, saveCreds } = await useMultiFileAuthState(process.env.DATA_PATH || './.baileys_auth');
    
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

// Safe API endpoint to get the current QR code data
app.get('/qr', (req, res) => {
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
    res.send(`
        <html>
            <head>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
                <style>
                    body { display:flex; justify-content:center; align-items:center; height:100vh; background:#0f172a; margin:0; font-family:sans-serif; color:white; }
                    .card { text-align:center; padding: 2rem; background:rgba(30,41,59,0.9); border-radius:16px; border:1px solid rgba(255,255,255,0.1); max-width:400px; }
                    #qrcode { margin: 20px auto; background: white; padding: 10px; border-radius: 8px; display:inline-block; }
                    #status { color:#94a3b8; font-size:14px; margin-top:10px; }
                </style>
            </head>
            <body>
                <div class="card">
                    <h2>Link your WhatsApp</h2>
                    <p>Scan this QR code with your WhatsApp app.</p>
                    <div id="qrcode"></div>
                    <p id="status">Loading...</p>
                </div>
                <script>
                    let currentQRText = '';
                    let pollTimer = null;
                    
                    async function checkStatus() {
                        try {
                            const res = await fetch('/qr');
                            const data = await res.json();
                            const statusEl = document.getElementById('status');
                            const qrEl = document.getElementById('qrcode');
                            
                            if (data.status === 'connected') {
                                statusEl.style.color = '#34d399';
                                statusEl.textContent = 'WhatsApp connected as ' + data.name + '!';
                                qrEl.innerHTML = '<div style="font-size:64px">&#9989;</div>';
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
        const { number, text, imageUrl, caption, buttons } = req.body;

        if (!number) {
            return res.status(400).json({ error: 'Number is required (e.g. 385919293138 or "me")' });
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
            await sock.sendMessage(chatId, {
                image: { url: imageUrl },
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
        const { phone, description } = req.body;
        console.log(`🌐 Received web form from ${phone}`);

        const formData = new FormData();
        formData.append('phone', phone);
        formData.append('description', description);

        if (req.file) {
            const blob = new Blob([req.file.buffer], { type: req.file.mimetype });
            formData.append('image', blob, req.file.originalname);
        }
        
        // Retry logic for Render cold starts (free tier sleeps after 15min)
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
                        console.error("FastAPI Error Response:", JSON.stringify(result, null, 2));
                        throw new Error(result.detail ? JSON.stringify(result.detail) : (result.message || 'FastAPI rejected the request'));
                    }
                    break; // Success!
                } else {
                    const text = await response.text();
                    console.log(`Attempt ${attempt}: Got non-JSON response (Status ${response.status}). Server may still be waking up...`);
                    lastError = `FastAPI returned status ${response.status} (not JSON). Server may be cold-starting.`;
                    if (attempt < maxRetries) {
                        await new Promise(r => setTimeout(r, 5000)); // Wait 5s before retry
                    }
                }
            } catch (fetchErr) {
                console.log(`Attempt ${attempt}: Fetch failed: ${fetchErr.message}`);
                lastError = fetchErr.message;
                if (attempt < maxRetries) {
                    await new Promise(r => setTimeout(r, 5000));
                }
            }
        }
        
        if (!result) {
            throw new Error(lastError || 'FastAPI server did not respond after retries.');
        }

        res.json({ success: true, result });
    } catch (error) {
        console.error("Web form processing error:", error);
        res.status(500).json({ error: error.message });
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`wbot API server running on port ${PORT}`);
});